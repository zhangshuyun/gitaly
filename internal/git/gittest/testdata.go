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
}

func ahmadSherif(ts int64) *gitalypb.CommitAuthor {
	return &gitalypb.CommitAuthor{
		Name:     []byte("Ahmad Sherif"),
		Email:    []byte("ahmad+gitlab-test@gitlab.com"),
		Date:     &timestamp.Timestamp{Seconds: ts},
		Timezone: []byte("+0200"),
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
