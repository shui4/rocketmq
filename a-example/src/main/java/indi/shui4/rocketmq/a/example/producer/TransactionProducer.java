package indi.shui4.rocketmq.a.example.producer;

/**
 * 事物
 *
 * @author shui4
 * @date 2022/3/16
 * @since 1.0.0
 */
public class TransactionProducer {
  public static void main(String[] args) {

    // 1) 准备消息
    // 执行本地事物

    // 2) 本地事物成功?
    // 本地事物-成功:消息commit,此时订阅方收到该消息
    // 本地事物-失败:消息rollback,此时订阅方收不到该消息

    // 本地事物成功提交,但消息commit的时候出现异常?比如未发送到RocketMQ
    // 服务器在经过固定时间段后将对"待确认"消息发起回查确认
    // 发送方法收到消息回查请求后(如果发送一段消息的Producer不能工作,回查请求将被发送到和Producer在同一个Group里的其它Producer),通过检查对应消息的本地事物执行结果返回commit或者rollback

    // 由于2)阶段的commit或者rollback会修改消息在Broker对应消息的状态,这样会造成磁盘Catch的脏页过多,降低系统的性能.
    // 所以RocketMQ在4.x的版本中将这部分功能去除.系统中在一些上层Class都还在,用户可以根据实际需求实现自己的事物功能
    //
    // LocalTransactionExecuter:用它去执行本地事物,根据情况返回LocalTransactionState.ROLLBACK_MESSAGE|COMMIT_MESSAGE
    //    TransactionMQProducer:用法和
    // DefaultMQProducer类似,要通过它启动一个Producer并发消息,但是比DefaultMQProducer多设置本地事物处理函数和回查状态函数
    // TransactionCheckListener:实现回查请求  LocalTransactionState.ROLLBACK_MESSAGE|COMMIT_MESSAGE
    // TransactionListener:https://zhuanlan.zhihu.com/p/249233648

  }
}
