# Editing Gitaly dashboards

Use the Grafana web interface to make changes to dashboards if
necessary.

**Remember to hit the 'Save' button at the top of the Grafana screen when making changes.**

## Tiled (repeated) dashboards

If you want to make a change across a tiled Grafana dashboard such as
the [feature request rate
overview](https://performance.gitlab.net/dashboard/db/gitaly-features-overview),
then edit the first tile (top left). Its settings get applied to the
other tiles as well. If you edit any tile other than the first your
changes will be lost.

## Drop-down values

At the top of most of our Grafana dashboards you will find drop-down menus
for GRPC method names, Prometheus jobs etc. The possible values in these
drop-downs are defined with Prometheus queries. To see or change these
queries, go into the dashboard's global settings (the gear icon at the
top of the page) and look in the 'Templating' section. You can then edit
entries.

Note that Grafana 'templates' use a combination of PromQL and
Grafana-specific modifiers.

# Ad-hoc latency graphs with ELK

Gitaly RPC latency data from Prometheus uses irregular (exponential)
bucket sizes which gives you unrealistic numbers. To get more realistic
percentiles you can use ELK.

-   Go to [ELK](https://log.gitlab.net)
-   Click 'Visualize'
-   Search for `gitaly rpc latency example`
-   Edit as needed
