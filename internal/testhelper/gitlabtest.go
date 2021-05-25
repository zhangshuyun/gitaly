package testhelper

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

var (
	/*
		This is a manually maintained map to remove duplicate variable
		assignments. Please do not use go generate or such to maintain
		these, as we'd effectively test one parser against another.
	*/
	commitMap = map[string]*gitalypb.GitCommit{
		"b83d6e391c22777fca1ed3012fce84f633d7fed0": &gitalypb.GitCommit{
			Id:      "b83d6e391c22777fca1ed3012fce84f633d7fed0",
			Subject: []byte("Merge branch 'branch-merged' into 'master'"),
			Body:    []byte("Merge branch 'branch-merged' into 'master'\r\n\r\nadds bar folder and branch-test text file to check Repository merged_to_root_ref method\r\n\r\n\r\n\r\nSee merge request !12"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Job van der Voort"),
				Email:    []byte("job@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474987066},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Job van der Voort"),
				Email:    []byte("job@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474987066},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{
				"1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
				"498214de67004b1da3d820901307bed2a68a8ef6",
			},
			BodySize: 162,
			TreeId:   "06a736b30226509bcb4bae41df06c9be8f93a716",
		},
		"e63f41fe459e62e1228fcef60d7189127aeba95a": &gitalypb.GitCommit{
			Id:      "e63f41fe459e62e1228fcef60d7189127aeba95a",
			Subject: []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'"),
			Body:    []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'\r\n\r\nUpdate README.md to include `Usage in testing and development`\r\n\r\nSee merge request !21"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Sean McGivern"),
				Email:    []byte("sean@mcgivern.me.uk"),
				Date:     &timestamp.Timestamp{Seconds: 1491906794},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Sean McGivern"),
				Email:    []byte("sean@mcgivern.me.uk"),
				Date:     &timestamp.Timestamp{Seconds: 1491906794},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{
				"b83d6e391c22777fca1ed3012fce84f633d7fed0",
				"4a24d82dbca5c11c61556f3b35ca472b7463187e",
			},
			BodySize: 154,
			TreeId:   "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
		},
		"4a24d82dbca5c11c61556f3b35ca472b7463187e": &gitalypb.GitCommit{
			Id:      "4a24d82dbca5c11c61556f3b35ca472b7463187e",
			Subject: []byte("Update README.md to include `Usage in testing and development`"),
			Body:    []byte("Update README.md to include `Usage in testing and development`"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Luke \"Jared\" Bennett"),
				Email:    []byte("lbennett@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1491905339},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Luke \"Jared\" Bennett"),
				Email:    []byte("lbennett@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1491905339},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{"b83d6e391c22777fca1ed3012fce84f633d7fed0"},
			BodySize:  62,
			TreeId:    "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
		},
		"ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69": &gitalypb.GitCommit{
			Id:      "ba3343bc4fa403a8dfbfcab7fc1a8c29ee34bd69",
			Subject: []byte("Weird commit date"),
			Body:    []byte("Weird commit date\n"),
			Author: &gitalypb.CommitAuthor{
				Name:  []byte("Alejandro Rodríguez"),
				Email: []byte("alejorro70@gmail.com"),
				// Not the actual commit date, but the biggest we can represent
				Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Alejandro Rodríguez"),
				Email:    []byte("alejorro70@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 9223371974719179007},
				Timezone: []byte("+0000"),
			},
			ParentIds: []string{"e63f41fe459e62e1228fcef60d7189127aeba95a"},
			BodySize:  18,
			TreeId:    "900a037dd45679f72b95e5198459260b232a0d13",
		},
		"498214de67004b1da3d820901307bed2a68a8ef6": &gitalypb.GitCommit{
			Id:      "498214de67004b1da3d820901307bed2a68a8ef6",
			Subject: []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method"),
			Body:    []byte("adds bar folder and branch-test text file to check Repository merged_to_root_ref method\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("tiagonbotelho"),
				Email:    []byte("tiagonbotelho@hotmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474470806},
				Timezone: []byte("+0100"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("tiagonbotelho"),
				Email:    []byte("tiagonbotelho@hotmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1474470806},
				Timezone: []byte("+0100"),
			},
			ParentIds: []string{"1b12f15a11fc6e62177bef08f47bc7b5ce50b141"},
			BodySize:  88,
			TreeId:    "06a736b30226509bcb4bae41df06c9be8f93a716",
		},
		"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9": &gitalypb.GitCommit{
			Id:      "6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9",
			Subject: []byte("More submodules"),
			Body:    []byte("More submodules\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491261},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491261},
				Timezone: []byte("+0200"),
			},
			ParentIds:     []string{"d14d6c0abdd253381df51a723d58691b2ee1ab08"},
			BodySize:      84,
			SignatureType: gitalypb.SignatureType_PGP,
			TreeId:        "70d69cce111b0e1f54f7e5438bbbba9511a8e23c",
		},
		"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863": &gitalypb.GitCommit{
			Id:      "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
			Subject: []byte("Initial commit"),
			Body:    []byte("Initial commit\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393488198},
				Timezone: []byte("-0800"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393488198},
				Timezone: []byte("-0800"),
			},
			ParentIds: nil,
			BodySize:  15,
			TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
		},
		"77e835ef0856f33c4f0982f84d10bdb0567fe440": &gitalypb.GitCommit{
			Id:      "77e835ef0856f33c4f0982f84d10bdb0567fe440",
			Subject: []byte("Add file larger than 1 mb"),
			Body:    []byte("Add file larger than 1 mb\n\nIn order to test Max File Size push rule we need a file larger than 1 MB\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Ruben Davila"),
				Email:    []byte("rdavila84@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1523247267},
				Timezone: []byte("-0500"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Jacob Vosmaer"),
				Email:    []byte("jacob@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1527855450},
				Timezone: []byte("+0200"),
			},
			ParentIds: []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
			BodySize:  100,
			TreeId:    "f9f4f0b6c70cbd88549d1e5b441ccd65b436a594",
		},
		"0999bb770f8dc92ab5581cc0b474b3e31a96bf5c": &gitalypb.GitCommit{
			Id:      "0999bb770f8dc92ab5581cc0b474b3e31a96bf5c",
			Subject: []byte("Hello\xf0world"),
			Body:    []byte("Hello\xf0world\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Jacob Vosmaer"),
				Email:    []byte("jacob@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1517328273},
				Timezone: []byte("+0100"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Jacob Vosmaer"),
				Email:    []byte("jacob@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1517328273},
				Timezone: []byte("+0100"),
			},
			ParentIds:     []string{"60ecb67744cb56576c30214ff52294f8ce2def98"},
			BodySize:      12,
			SignatureType: gitalypb.SignatureType_NONE,
			TreeId:        "7e2f26d033ee47cd0745649d1a28277c56197921",
		},
		"189a6c924013fc3fe40d6f1ec1dc20214183bc97": &gitalypb.GitCommit{
			Id:      "189a6c924013fc3fe40d6f1ec1dc20214183bc97",
			Subject: []byte("style: use markdown header within README.md"),
			Body:    []byte("style: use markdown header within README.md\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Roger Meier"),
				Email:    []byte("r.meier@siemens.com"),
				Date:     &timestamp.Timestamp{Seconds: 1570810009},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Roger Meier"),
				Email:    []byte("r.meier@siemens.com"),
				Date:     &timestamp.Timestamp{Seconds: 1570810009},
				Timezone: []byte("+0200"),
			},
			ParentIds:     []string{"0ad583fecb2fb1eaaadaf77d5a33bc69ec1061c1"},
			BodySize:      44,
			SignatureType: gitalypb.SignatureType_X509,
			TreeId:        "13d7469f409bd0b8580a4f62c04dc0e710201136",
		},
		"570e7b2abdd848b95f2f578043fc23bd6f6fd24d": &gitalypb.GitCommit{
			Id:      "570e7b2abdd848b95f2f578043fc23bd6f6fd24d",
			Subject: []byte("Change some files"),
			Body:    []byte("Change some files\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491451},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491451},
				Timezone: []byte("+0200"),
			},
			ParentIds:     []string{"6f6d7e7ed97bb5f0054f2b1df789b39ca89b6ff9"},
			BodySize:      86,
			SignatureType: gitalypb.SignatureType_PGP,
			TreeId:        "842b021b36723db3cf52936d3ff7c566d36c108c",
		},
		"5937ac0a7beb003549fc5fd26fc247adbce4a52e": &gitalypb.GitCommit{
			Id:      "5937ac0a7beb003549fc5fd26fc247adbce4a52e",
			Subject: []byte("Add submodule from gitlab.com"),
			Body:    []byte("Add submodule from gitlab.com\n\nSigned-off-by: Dmitriy Zaporozhets <dmitriy.zaporozhets@gmail.com>\n"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491698},
				Timezone: []byte("+0200"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Dmitriy Zaporozhets"),
				Email:    []byte("dmitriy.zaporozhets@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1393491698},
				Timezone: []byte("+0200"),
			},
			ParentIds:     []string{"570e7b2abdd848b95f2f578043fc23bd6f6fd24d"},
			BodySize:      98,
			SignatureType: gitalypb.SignatureType_PGP,
			TreeId:        "a6973545d42361b28bfba5ced3b75dba5848b955",
		},
		"1b12f15a11fc6e62177bef08f47bc7b5ce50b141": &gitalypb.GitCommit{
			Id:        "1b12f15a11fc6e62177bef08f47bc7b5ce50b141",
			Body:      []byte("Merge branch 'add-directory-with-space' into 'master'\r\n\r\nAdd a directory containing a space in its name\r\n\r\nneeded for verifying the fix of `https://gitlab.com/gitlab-com/support-forum/issues/952` \r\n\r\nSee merge request !11"),
			BodySize:  221,
			ParentIds: []string{"6907208d755b60ebeacb2e9dfea74c92c3449a1f", "38008cb17ce1466d8fec2dfa6f6ab8dcfe5cf49e"},
			Subject:   []byte("Merge branch 'add-directory-with-space' into 'master'"),
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Stan Hu"),
				Email:    []byte("stanhu@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1471558878},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Stan Hu"),
				Email:    []byte("stanhu@gmail.com"),
				Date:     &timestamp.Timestamp{Seconds: 1471558878},
				Timezone: []byte("+0000"),
			},
			TreeId: "23f60b6e4ff0c59039b42c8fc4d3f008abef3bee",
		},
		"e56497bb5f03a90a51293fc6d516788730953899": &gitalypb.GitCommit{
			Id:        "e56497bb5f03a90a51293fc6d516788730953899",
			Subject:   []byte("Merge branch 'tree_helper_spec' into 'master'"),
			Body:      []byte("Merge branch 'tree_helper_spec' into 'master'\n\nAdd directory structure for tree_helper spec\n\nThis directory structure is needed for a testing the method flatten_tree(tree) in the TreeHelper module\n\nSee [merge request #275](https://gitlab.com/gitlab-org/gitlab-ce/merge_requests/275#note_732774)\n\nSee merge request !2\n"),
			BodySize:  317,
			ParentIds: []string{"5937ac0a7beb003549fc5fd26fc247adbce4a52e", "4cd80ccab63c82b4bad16faa5193fbd2aa06df40"},
			Author: &gitalypb.CommitAuthor{
				Name:     []byte("Sytse Sijbrandij"),
				Email:    []byte("sytse@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1420925009},
				Timezone: []byte("+0000"),
			},
			Committer: &gitalypb.CommitAuthor{
				Name:     []byte("Sytse Sijbrandij"),
				Email:    []byte("sytse@gitlab.com"),
				Date:     &timestamp.Timestamp{Seconds: 1420925009},
				Timezone: []byte("+0000"),
			},
			TreeId: "c56b5e763e885e1aed626da52a603ba740936ac2",
		},
	}
)

// GitLabTestCommit provides a key value lookup for commits in the GitLab-Test
// repository
func GitLabTestCommit(id string) *gitalypb.GitCommit {
	return commitMap[id]
}
