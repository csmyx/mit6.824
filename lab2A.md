- timer 设计思路：
    1. 在goroutine中
- 访问rf.peers和rf.me不需要互斥，因为只有并发读，没有写
- RequestVote RPC应该可以共享一个Args，因为对Args只并发读
									
- 在刚开始选举时需要记录此刻的rf.term值(记录term0)与后面作对比，因为requestvote goroutine的执行有延迟(需获得锁)，期间全局最新term可能已经被更新。如果在voteRPC收到回复时对比前面记录的term0， 
    - 1) 发现rf.term已经被更新(即rf.term > term0，可能是因为此轮选举有人先我一步成为leader，并向我发送了心跳包，然后我在处理心跳包时，更新了rf.term，此外还变为了follower)，说明此轮投票已经过期，所以直接丢弃投票结果，不作处理 // 通过处理 “hearbeat RPC请求” (我是RPC Callee)发现term已经更新(即leader已经先我一步选出)，**在处理hearbeat RPC请求时更新rf.term，以及从candidate转换为follower**，
    - 2) 若rf.term == term0，但是发现requestvote RPC回复里 reply.term > rf.term，则更新rf.term，同时从candidate转换为follower // 通过处理“requestvote RPC回复” (我是RPC Caller)，发现term已经更新(即leader已经先我一步选出)，**在收到requestvote RPC回复时更新rf.term，以及从candidate转换为follower**，
- 如果cadidate在处理vote RPC结果时发现term(无论是reply.term还是rf.term自身)等于term0未更新，且收到majority of vote，那么说明一定没有人先于我成为leader(若存在，那它一定提前获得了majority of vote，加上我的majority of vote，总投票数与每个server每轮最多投1次票矛盾)，所以我一定能成为此轮的leader

- 如果RPC双方的term不相等，其中较小的一方会更新自己的term(无论它是sender还是receiver，若是sender会在收到回复时更新)
- 如果是candidate/leader的term较小，则会立即变为follower状态

- candidate选举成功(即获得majority of vote)后，需要立即发送AppendEntry(heatbeat)。别的server是通过该hartbeat得知"本轮leader已经选出"的消息的(因为只有leader才能发送AppednEntry RPC)