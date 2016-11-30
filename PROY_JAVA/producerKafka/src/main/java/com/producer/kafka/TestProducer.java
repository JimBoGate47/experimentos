package com.producer.kafka;

import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class TestProducer {
    public static void main(String[] args) {

        Integer N = Integer.parseInt(args[0]);
        //int N=5;
        Properties props = new Properties();
        props.put("bootstrap.servers","localhost:9092");
        props.put("acks","all");
        props.put("retries",0);
        props.put("batch.size",16384);
        props.put("linger.ms",1);
        props.put("buffer.memory",33554432);
        props.put("key.serializer","org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer","org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);


        String [] argus={"AE"," "," "," "," "," "," "};
        String [] zones={"IT","HVS","Electronics"};
        String [] vargus=new String[argus.length];

        String f;
        String [] tmp;
        LocalDateTime time;
        DateTimeFormatter formDT= DateTimeFormatter.ISO_DATE_TIME;

        Message crear = new Message(argus);
        Random rnd = new Random();
        NumberFormat formatter= new DecimalFormat("###.#");


        for (int i =0;i<N;i++){
            f = formatter.format(1e4 + rnd.nextInt(100));
            time=LocalDateTime.now();

            vargus[0] = f;
            vargus[1] = zones[rnd.nextInt(3)];
            vargus[2] = vargus[1] + "_" + rnd.nextInt(5);
            vargus[3] = String.valueOf(rnd.nextInt(10) + rnd.nextFloat());
            vargus[4] = String.valueOf(rnd.nextInt(20) + rnd.nextFloat());
            vargus[5] = String.valueOf(rnd.nextInt(15) + rnd.nextFloat());
            vargus[6] = time.toString();

            tmp = crear.create(vargus);

   //         System.out.println(tmp[0]  + tmp[1]);
            producer.send(new ProducerRecord<>("my-topic",tmp[0],tmp[0] + tmp[1]));
        }
        producer.close();
    }
}
