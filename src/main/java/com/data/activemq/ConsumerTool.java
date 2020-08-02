package com.data.activemq;
import javax.jms.Connection;      
import javax.jms.Destination;      
import javax.jms.ExceptionListener;
import javax.jms.JMSException;      
import javax.jms.MessageConsumer;      
import javax.jms.Session;      
import javax.jms.MessageListener;      
import javax.jms.Message;      
import javax.jms.TextMessage;      
     
import org.apache.activemq.ActiveMQConnection;      
import org.apache.activemq.ActiveMQConnectionFactory;      
     
public class ConsumerTool implements MessageListener,ExceptionListener {
    private String user = ActiveMQConnection.DEFAULT_USER;//连接用户（这里是一个默认的）
    private String password = ActiveMQConnection.DEFAULT_PASSWORD; //连接密码（这里是一个默认的）
    private String url = "failover://tcp://172.16.106.128:61616";//连接URl（这里是一个默认的）
    private String subject = "mytopic";//主题
    private Destination destination = null;      
    private Connection connection = null;      
    private Session session = null;      
    private MessageConsumer consumer = null;  
    public static Boolean isconnection=false;

    // 初始化      
    private void initialize() throws JMSException, Exception {      
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(      
                user, password, url);//连接工厂
        connection = connectionFactory.createConnection();//创建连接
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);//创建一个会话
        destination = session.createTopic(subject);//会话创建一个主题
        consumer = session.createConsumer(destination);//创建一个消费者
    }      
     
    // 消费消息      
    public void consumeMessage() throws JMSException, Exception {      
        initialize();      
        connection.start();//连接启动
        consumer.setMessageListener(this);//设置一个监听
        connection.setExceptionListener(this);//设置一个异常监听
        isconnection=true;
        System.out.println("Consumer:->Begin listening...");      
        // 开始监听  
        // Message message = consumer.receive();      
    }
    // 关闭连接      
    public void close() throws JMSException {      
        System.out.println("Consumer:->Closing connection");      
        if (consumer != null)      
            consumer.close();      
        if (session != null)      
            session.close();      
        if (connection != null)      
            connection.close();      
    }
    // 消息处理函数
    //监听处理（设置监听后，会调用此方法）
    public void onMessage(Message message) {      
        try {
            //获取生产者消息
            if (message instanceof TextMessage) {
                //根据消息转换类型
                TextMessage txtMsg = (TextMessage) message;      
                String msg = txtMsg.getText();      
                System.out.println("Consumer:->Received: " + msg);      
            } else {      
                System.out.println("Consumer:->Received: " + message);      
            }      
        } catch (JMSException e) {      
            // TODO Auto-generated catch block      
            e.printStackTrace();      
        }      
    }

    //异常监听处理（设置异常监听后，会调用此方法）
	public void onException(JMSException arg0) {
		isconnection=false;
	}      
}      
     
