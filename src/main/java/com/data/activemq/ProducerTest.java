package com.data.activemq;
import java.util.Random;

import javax.jms.JMSException;      

public class ProducerTest {      
     
    /**    
     * @param args    
     */     
    public static void main(String[] args) throws JMSException, Exception {      
        ProducerTool producer = new ProducerTool(); 
        Random random = new Random();
        for(int i=0;i<5;i++){

        	Thread.sleep(random.nextInt(10)*1000);
        	
        	producer.produceMessage("Hello, world!--"+i);      
        	producer.close();
        }
        
    }      
}      

