package datamanager

// 管理策略

// 1个loader线程
// go service.NewPolicyService().LoadFactory()(ctx)
// // 10个writer线程
// for i := 0; i < 10; i++ {
// 	go service.NewPolicyService().WriteFactory()(ctx)
// }
