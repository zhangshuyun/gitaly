package gittest

import (
	"github.com/golang/protobuf/ptypes/timestamp"
	"gitlab.com/gitlab-org/gitaly/v14/proto/go/gitalypb"
)

// CommitsByID is a map of GitCommit structures by their respective IDs.
var CommitsByID = map[string]*gitalypb.GitCommit{
	"0031876facac3f2b2702a0e53a26e89939a42209": &gitalypb.GitCommit{
		Id:        "0031876facac3f2b2702a0e53a26e89939a42209",
		Subject:   []byte("Merge branch 'few-commits-4' into few-commits-2"),
		Body:      []byte("Merge branch 'few-commits-4' into few-commits-2\n"),
		Author:    ahmadSherif(1500320762),
		Committer: ahmadSherif(1500320762),
		ParentIds: []string{
			"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
			"48ca272b947f49eee601639d743784a176574a09",
		},
		BodySize: 48,
		TreeId:   "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"48ca272b947f49eee601639d743784a176574a09": &gitalypb.GitCommit{
		Id:        "48ca272b947f49eee601639d743784a176574a09",
		Subject:   []byte("Commit #9 alternate"),
		Body:      []byte("Commit #9 alternate\n"),
		Author:    ahmadSherif(1500320271),
		Committer: ahmadSherif(1500320271),
		ParentIds: []string{"335bc94d5b7369b10251e612158da2e4a4aaa2a5"},
		BodySize:  20,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"335bc94d5b7369b10251e612158da2e4a4aaa2a5": &gitalypb.GitCommit{
		Id:        "335bc94d5b7369b10251e612158da2e4a4aaa2a5",
		Subject:   []byte("Commit #8 alternate"),
		Body:      []byte("Commit #8 alternate\n"),
		Author:    ahmadSherif(1500320269),
		Committer: ahmadSherif(1500320269),
		ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
		BodySize:  20,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb": &gitalypb.GitCommit{
		Id:        "bf6e164cac2dc32b1f391ca4290badcbe4ffc5fb",
		Subject:   []byte("Commit #10"),
		Body:      []byte("Commit #10\n"),
		Author:    ahmadSherif(1500320272),
		Committer: ahmadSherif(1500320272),
		ParentIds: []string{"9d526f87b82e2b2fd231ca44c95508e5e85624ca"},
		BodySize:  11,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"9d526f87b82e2b2fd231ca44c95508e5e85624ca": &gitalypb.GitCommit{
		Id:        "9d526f87b82e2b2fd231ca44c95508e5e85624ca",
		Subject:   []byte("Commit #9"),
		Body:      []byte("Commit #9\n"),
		Author:    ahmadSherif(1500320270),
		Committer: ahmadSherif(1500320270),
		ParentIds: []string{"1039376155a0d507eba0ea95c29f8f5b983ea34b"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"1039376155a0d507eba0ea95c29f8f5b983ea34b": &gitalypb.GitCommit{
		Id:        "1039376155a0d507eba0ea95c29f8f5b983ea34b",
		Subject:   []byte("Commit #8"),
		Body:      []byte("Commit #8\n"),
		Author:    ahmadSherif(1500320268),
		Committer: ahmadSherif(1500320268),
		ParentIds: []string{"54188278422b1fa877c2e71c4e37fc6640a58ad1"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"54188278422b1fa877c2e71c4e37fc6640a58ad1": &gitalypb.GitCommit{
		Id:        "54188278422b1fa877c2e71c4e37fc6640a58ad1",
		Subject:   []byte("Commit #7"),
		Body:      []byte("Commit #7\n"),
		Author:    ahmadSherif(1500320266),
		Committer: ahmadSherif(1500320266),
		ParentIds: []string{"8b9270332688d58e25206601900ee5618fab2390"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"8b9270332688d58e25206601900ee5618fab2390": &gitalypb.GitCommit{
		Id:        "8b9270332688d58e25206601900ee5618fab2390",
		Subject:   []byte("Commit #6"),
		Body:      []byte("Commit #6\n"),
		Author:    ahmadSherif(1500320264),
		Committer: ahmadSherif(1500320264),
		ParentIds: []string{"f9220df47bce1530e90c189064d301bfc8ceb5ab"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"f9220df47bce1530e90c189064d301bfc8ceb5ab": &gitalypb.GitCommit{
		Id:        "f9220df47bce1530e90c189064d301bfc8ceb5ab",
		Subject:   []byte("Commit #5"),
		Body:      []byte("Commit #5\n"),
		Author:    ahmadSherif(1500320262),
		Committer: ahmadSherif(1500320262),
		ParentIds: []string{"40d408f89c1fd26b7d02e891568f880afe06a9f8"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"40d408f89c1fd26b7d02e891568f880afe06a9f8": &gitalypb.GitCommit{
		Id:        "40d408f89c1fd26b7d02e891568f880afe06a9f8",
		Subject:   []byte("Commit #4"),
		Body:      []byte("Commit #4\n"),
		Author:    ahmadSherif(1500320260),
		Committer: ahmadSherif(1500320260),
		ParentIds: []string{"df914c609a1e16d7d68e4a61777ff5d6f6b6fde3"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"df914c609a1e16d7d68e4a61777ff5d6f6b6fde3": &gitalypb.GitCommit{
		Id:        "df914c609a1e16d7d68e4a61777ff5d6f6b6fde3",
		Subject:   []byte("Commit #3"),
		Body:      []byte("Commit #3\n"),
		Author:    ahmadSherif(1500320258),
		Committer: ahmadSherif(1500320258),
		ParentIds: []string{"6762605237fc246ae146ac64ecb467f71d609120"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"6762605237fc246ae146ac64ecb467f71d609120": &gitalypb.GitCommit{
		Id:        "6762605237fc246ae146ac64ecb467f71d609120",
		Subject:   []byte("Commit #2"),
		Body:      []byte("Commit #2\n"),
		Author:    ahmadSherif(1500320256),
		Committer: ahmadSherif(1500320256),
		ParentIds: []string{"79b06233d3dc769921576771a4e8bee4b439595d"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"79b06233d3dc769921576771a4e8bee4b439595d": &gitalypb.GitCommit{
		Id:        "79b06233d3dc769921576771a4e8bee4b439595d",
		Subject:   []byte("Commit #1"),
		Body:      []byte("Commit #1\n"),
		Author:    ahmadSherif(1500320254),
		Committer: ahmadSherif(1500320254),
		ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		BodySize:  10,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863": &gitalypb.GitCommit{
		Id:        "1a0b36b3cdad1d2ee32457c102a8c0b7056fa863",
		Subject:   []byte("Initial commit"),
		Body:      []byte("Initial commit\n"),
		Author:    dmitriyZaporozhets(1393488198),
		Committer: dmitriyZaporozhets(1393488198),
		ParentIds: nil,
		BodySize:  15,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"304d257dcb821665ab5110318fc58a007bd104ed": &gitalypb.GitCommit{
		Id:        "304d257dcb821665ab5110318fc58a007bd104ed",
		Subject:   []byte("Commit #11"),
		Body:      []byte("Commit #11\n"),
		Author:    ahmadSherif(1500322381),
		Committer: ahmadSherif(1500322381),
		ParentIds: []string{"1a0b36b3cdad1d2ee32457c102a8c0b7056fa863"},
		BodySize:  11,
		TreeId:    "91639b9835ff541f312fd2735f639a50bf35d472",
	},
	"1e292f8fedd741b75372e19097c76d327140c312": &gitalypb.GitCommit{
		Id:        "1e292f8fedd741b75372e19097c76d327140c312",
		Subject:   []byte("Merge branch 'cherry-pikc-ce369011' into 'master'"),
		Body:      []byte("Merge branch 'cherry-pikc-ce369011' into 'master'\n\nAdd file with a _flattable_ path\n\n See merge request gitlab-org/gitlab-test!35\n"),
		Author:    drewBlessing(1540830087),
		Committer: drewBlessing(1540830087),
		ParentIds: []string{
			"79b06233d3dc769921576771a4e8bee4b439595d",
			"c1c67abbaf91f624347bb3ae96eabe3a1b742478",
		},
		BodySize: 388,
		TreeId:   "07f8147e8e73aab6c935c296e8cdc5194dee729b",
	},
	"60ecb67744cb56576c30214ff52294f8ce2def98": &gitalypb.GitCommit{
		Id:        "60ecb67744cb56576c30214ff52294f8ce2def98",
		Subject:   []byte("Merge branch 'lfs' into 'master'"),
		Body:      []byte("Merge branch 'lfs' into 'master'\n\nAdd LFS tracking of \"*.lfs\" to .gitattributes\n\nSee merge request gitlab-org/gitlab-test!28"),
		Author:    stanHu(1515740810),
		Committer: stanHu(1515740810),
		ParentIds: []string{
			"e63f41fe459e62e1228fcef60d7189127aeba95a",
			"55bc176024cfa3baaceb71db584c7e5df900ea65",
		},
		BodySize: 124,
		TreeId:   "7e2f26d033ee47cd0745649d1a28277c56197921",
	},
	"e63f41fe459e62e1228fcef60d7189127aeba95a": &gitalypb.GitCommit{
		Id:        "e63f41fe459e62e1228fcef60d7189127aeba95a",
		Subject:   []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'"),
		Body:      []byte("Merge branch 'gitlab-test-usage-dev-testing-docs' into 'master'\r\n\r\nUpdate README.md to include `Usage in testing and development`\r\n\r\nSee merge request !21"),
		Author:    seanMcGivern(1491906794),
		Committer: seanMcGivern(1491906794),
		ParentIds: []string{
			"b83d6e391c22777fca1ed3012fce84f633d7fed0",
			"4a24d82dbca5c11c61556f3b35ca472b7463187e",
		},
		BodySize: 154,
		TreeId:   "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
	},
	"55bc176024cfa3baaceb71db584c7e5df900ea65": &gitalypb.GitCommit{
		Id:        "55bc176024cfa3baaceb71db584c7e5df900ea65",
		Subject:   []byte("LFS tracks \"*.lfs\" through .gitattributes"),
		Body:      []byte("LFS tracks \"*.lfs\" through .gitattributes\n"),
		Author:    jamesEdwardsJones(1515687321),
		Committer: jamesEdwardsJones(1515738427),
		ParentIds: []string{
			"b83d6e391c22777fca1ed3012fce84f633d7fed0",
		},
		BodySize: 42,
		TreeId:   "1970c07e0e1ce7fcf82edc2e3792564bd8ea3744",
	},
	"4a24d82dbca5c11c61556f3b35ca472b7463187e": &gitalypb.GitCommit{
		Id:        "4a24d82dbca5c11c61556f3b35ca472b7463187e",
		Subject:   []byte("Update README.md to include `Usage in testing and development`"),
		Body:      []byte("Update README.md to include `Usage in testing and development`"),
		Author:    lukeBennett(1491905339),
		Committer: lukeBennett(1491905339),
		ParentIds: []string{
			"b83d6e391c22777fca1ed3012fce84f633d7fed0",
		},
		BodySize: 62,
		TreeId:   "86ec18bfe87ad42a782fdabd8310f9b7ac750f51",
	},
	"ce369011c189f62c815f5971d096b26759bab0d1": &gitalypb.GitCommit{
		Id:        "ce369011c189f62c815f5971d096b26759bab0d1",
		Subject:   []byte("Add file with a _flattable_ path"),
		Body:      []byte("Add file with a _flattable_ path\n"),
		Author:    alejandroRodriguez(1504382739),
		Committer: alejandroRodriguez(1504397760),
		ParentIds: []string{
			"913c66a37b4a45b9769037c55c2d238bd0942d2e",
		},
		BodySize: 33,
		TreeId:   "729bb692f55d49149609dd1ceaaf1febbdec7d0d",
	},
}

func alejandroRodriguez(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Alejandro Rodríguez"),
		Email:    []byte("alejorro70@gmail.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("-0300"),
	}
}

func ahmadSherif(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad+gitlab-test@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0200"),
	}
}

func drewBlessing(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Drew Blessing"),
		Email:    []byte("drew@blessing.io"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0000"),
	}
}

func dmitriyZaporozhets(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Dmitriy Zaporozhets"),
		Email:    []byte("dmitriy.zaporozhets@gmail.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("-0800"),
	}
}

func jamesEdwardsJones(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("James Edwards-Jones"),
		Email:    []byte("jedwardsjones@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0000"),
	}
}

func lukeBennett(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Luke \"Jared\" Bennett"),
		Email:    []byte("lbennett@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0000"),
	}
}

func seanMcGivern(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Sean McGivern"),
		Email:    []byte("sean@mcgivern.me.uk"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0000"),
	}
}

func stanHu(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Stan Hu"),
		Email:    []byte("stanhu@gmail.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0000"),
	}
}
