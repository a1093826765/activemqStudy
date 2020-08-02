package com.data.activemq;
import javax.jms.Connection;      
import javax.jms.DeliveryMode;      
import javax.jms.Destination;      
import javax.jms.JMSException;      
import javax.jms.MessageProducer;      
import javax.jms.Session;      
import javax.jms.TextMessage;      
     
import org.apache.activemq.ActiveMQConnection;      
import org.apache.activemq.ActiveMQConnectionFactory;      
     
public class ProducerTool {
    private String user = ActiveMQConnection.DEFAULT_USER;//连接用户（这里是一个默认的）
    private String password = ActiveMQConnection.DEFAULT_PASSWORD; //连接密码（这里是一个默认的）
    private String url = "failover://tcp://172.16.106.128:61616";//连接URl（这里是一个默认的）
    private String subject = "mytopic";//主题
    private Destination destination = null;      
    private Connection connection = null;      
    private Session session = null;      
    private MessageProducer producer = null;


    // 初始化      
    private void initialize() throws JMSException, Exception {      
        ActiveMQConnectionFactory connectionFactory = new ActiveMQConnectionFactory(      
                user, password, url);//连接工厂
        connection = connectionFactory.createConnection();//创建连接
        session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);//创建一个会话
        destination = session.createTopic(subject);//会话创建一个主题
        producer = session.createProducer(destination);//创建一个生产者
        producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);//发送模式
    }

    // 发送消息      
    public void produceMessage(String message) throws JMSException, Exception {      
        initialize();      
        TextMessage msg = session.createTextMessage(message);//创建一个消息，封装文本消息
        connection.start();//连接启动
        System.out.println("Producer:->Sending message: " + message);      
        producer.send(msg);//发送消息
        System.out.println("Producer:->Message sent complete!");      
    }

    // 关闭连接      
    public void close() throws JMSException {      
        System.out.println("Producer:->Closing connection");      
        if (producer != null)      
            producer.close();
        if (session != null)      
            session.close();      
        if (connection != null)      
            connection.close();      
    }      
}        