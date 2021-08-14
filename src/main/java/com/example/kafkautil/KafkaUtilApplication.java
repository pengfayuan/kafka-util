package com.example.kafkautil;

import cn.hutool.core.io.FileUtil;
import cn.hutool.core.util.CharsetUtil;
import java.util.List;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
@Slf4j
public class KafkaUtilApplication {

    public static void main(String[] args) {
        KafkaProducer producer = new KafkaProducer();
        String filePath = args[0];
        List<String> lines = FileUtil.readLines(filePath, CharsetUtil.UTF_8);

        lines.stream().forEach(line -> {
            String[] split = line.split(",");
            if (split.length == 2) {
                String topic = "tmp." + split[0];
                String message = split[1];
                if (topic.contains("topic.out.Product_SelectStock")) {
                    producer.sendMessage(topic, message);
                    return;
                }

                String[] split2 = message.split("\\|");
                String time = split2[split2.length - 2];
                if (Integer.valueOf(time) > 1445) {
                    producer.sendMessage(topic, message);
                }
            }
        });
        System.out.println("over");
        SpringApplication.run(KafkaUtilApplication.class, args);

    }

}
