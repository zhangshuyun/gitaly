variable "project" { default = "gitlab-internal-153318" }
variable "demo_region" { default = "us-east4" }
variable "demo_zone" { default = "us-east4-c" }
variable "praefect_demo_cluster_name" { }
variable "ssh_user" { }
variable "ssh_pubkey" { }
variable "os_image" { default = "ubuntu-os-cloud/ubuntu-1804-lts" }
variable "gitlab_root_password" { }
variable "startup_script" {
  default = <<EOF
    set -e
    if [ -d /opt/gitlab ] ; then exit; fi

    curl -s https://packages.gitlab.com/install/repositories/gitlab/nightly-builds/script.deb.sh | sudo bash
    sudo apt-get install -y gitlab-ee
  EOF
}
variable "gitaly_machine_type" { default = "n1-standard-2" }
variable "praefect_machine_type" { default = "n1-standard-1" }
variable "gitaly_disk_size" { default = "100" }
variable "praefect_disk_size" { default = "10" }
variable "praefect_sql_password" { }

provider "google" {
  version = "~> 3.90"

  project = var.project
  region  = var.demo_region
  zone    = var.demo_zone
}

resource "random_id" "db_name_suffix" {
  byte_length = 4
}

resource "google_compute_address" "pgbouncer" {
  name = "${var.praefect_demo_cluster_name}-praefect-pgbouncer"
}

resource "google_sql_database_instance" "praefect_sql" {
  # It appears CloudSQL does not like Terraform re-using database names.
  # Adding a random ID prevents name reuse.
  name = "${var.praefect_demo_cluster_name}-praefect-postgresql-${random_id.db_name_suffix.hex}"
  database_version = "POSTGRES_12"
  region = var.demo_region
  deletion_protection = false

  settings {
    tier = "db-f1-micro"

    database_flags {
      name  = "log_min_duration_statement"
      value = "500"
    }

    ip_configuration{
      ipv4_enabled = true

      authorized_networks {
        name  = "allow-${google_compute_address.pgbouncer.name}"
        value = google_compute_address.pgbouncer.address
      }

      dynamic authorized_networks {
        for_each = google_compute_instance.praefect
        iterator = praefects

        content {
          name = "allow-${praefects.value.name}"
          value = praefects.value.network_interface.0.access_config.0.nat_ip
        }
      }
    }
  }
}

output "praefect_pgbouncer_ip" {
  value = module.pgbouncer.private_ip_address
}

output "praefect_postgres_ip" {
  value = google_sql_database_instance.praefect_sql.public_ip_address
}

resource "google_sql_user" "users" {
  name     = "praefect"
  instance = google_sql_database_instance.praefect_sql.name
  password = var.praefect_sql_password
  deletion_policy = "ABANDON"
}

resource "google_sql_database" "praefect-database" {
  name     = "praefect_production"
  instance = google_sql_database_instance.praefect_sql.name
}

module "pgbouncer" {
  source  = "christippett/cloud-sql-pgbouncer/google"
  version = "~>1.1"

  project    = var.project
  name       = "${var.praefect_demo_cluster_name}-pgbouncer"
  zone       = var.demo_zone
  subnetwork = "default"
  public_ip_address = google_compute_address.pgbouncer.address

  port          = 5432
  database_host = google_sql_database_instance.praefect_sql.public_ip_address

  users = [
    { name = google_sql_user.users.name, password = google_sql_user.users.password, admin = true },
  ]
  auth_query = "SELECT usename, passwd FROM pg_shadow WHERE usename=$1"
}

resource "google_compute_instance" "gitlab" {
  name         = format("%s-gitlab", var.praefect_demo_cluster_name)
  machine_type = "n1-standard-2"

  boot_disk {
    initialize_params {
      image = var.os_image
      size = var.gitaly_disk_size
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = format("%s:%s", var.ssh_user, var.ssh_pubkey)
    startup-script = <<EOF
      ${var.startup_script}
      GITLAB_ROOT_PASSWORD=${var.gitlab_root_password} gitlab-ctl reconfigure
    EOF
  }

  tags = ["http-server", "https-server"]
}

output "gitlab_internal_ip" {
  value = google_compute_instance.gitlab.network_interface[0].network_ip
}
output "gitlab_external_ip" {
  value = google_compute_instance.gitlab.network_interface[0].access_config[0].nat_ip
}

resource "google_compute_instance" "praefect" {
  count = 3
  name         =  "${var.praefect_demo_cluster_name}-praefect-${count.index + 1}"
  machine_type = var.praefect_machine_type

  boot_disk {
    initialize_params {
      image = var.os_image
      size = var.praefect_disk_size
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = format("%s:%s", var.ssh_user, var.ssh_pubkey)
    startup-script = var.startup_script
  }
}

resource "google_compute_instance_group" "praefect-cluster" {
  name = "${var.praefect_demo_cluster_name}-praefect-cluster"

  instances = google_compute_instance.praefect.*.self_link

  named_port {
    name = "praefect-transport"
    port = "2305"
  }
}

resource "google_compute_forwarding_rule" "praefect-forwarding-rule" {
  name                  = "${var.praefect_demo_cluster_name}-praefect-lb"
  load_balancing_scheme = "INTERNAL"
  backend_service       = google_compute_region_backend_service.praefect-lb.self_link
  ports                 = ["2305"]
}

resource "google_compute_region_backend_service" "praefect-lb" {
  name             = "${var.praefect_demo_cluster_name}-praefect-lb"
  protocol         = "TCP"
  timeout_sec      = 10
  session_affinity = "NONE"

  backend {
    group = google_compute_instance_group.praefect-cluster.self_link
  }

  health_checks = [
    google_compute_health_check.praefect-healthcheck.self_link
  ]
}

resource "google_compute_health_check" "praefect-healthcheck" {
  name = "${var.praefect_demo_cluster_name}-praefect-healthcheck"

  check_interval_sec = 5
  timeout_sec        = 5

  tcp_health_check {
    port = "2305"
  }
}

output "praefect_loadbalancer_ip" {
  value = google_compute_forwarding_rule.praefect-forwarding-rule.ip_address
}

output "praefect_internal_ip" {
  value = {
    for instance in google_compute_instance.praefect:
    instance.name => instance.network_interface[0].network_ip
  }
}

output "praefect_ssh_ip" {
  value = {
    for instance in google_compute_instance.praefect:
    instance.name => instance.network_interface[0].access_config[0].nat_ip
  }
}

resource "google_compute_instance" "gitaly" {
  count = 3
  name         =  "${var.praefect_demo_cluster_name}-gitaly-${count.index + 1}"
  machine_type = var.gitaly_machine_type

  boot_disk {
    initialize_params {
      image = var.os_image
      size = var.gitaly_disk_size
    }
  }

  network_interface {
    subnetwork = "default"
    access_config {}
  }

  metadata = {
    ssh-keys = format("%s:%s", var.ssh_user, var.ssh_pubkey)
    startup-script = var.startup_script
  }
}

output "gitaly_internal_ip" {
  value = {
    for instance in google_compute_instance.gitaly:
    instance.name => instance.network_interface[0].network_ip
  }
}

output "gitaly_ssh_ip" {
  value = {
    for instance in google_compute_instance.gitaly:
    instance.name => instance.network_interface[0].access_config[0].nat_ip
  }
}
