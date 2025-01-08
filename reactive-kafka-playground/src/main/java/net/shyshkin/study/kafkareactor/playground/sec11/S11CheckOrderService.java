package net.shyshkin.study.kafkareactor.playground.sec11;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class S11CheckOrderService {

    private static final Logger log = LoggerFactory.getLogger(S11CheckOrderService.class);

    private static final Map<String, Integer> maxNumber = new HashMap<>();

    public static boolean orderMatch(String key, String orderValue) {

        synchronized (maxNumber) {
            int orderNumber = Integer.parseInt(orderValue.replace("order-", ""));

            Integer val = maxNumber.get(key);
            if (val == null || val < orderNumber) {
                maxNumber.put(key, orderNumber);
                return true;
            }
            return false;
        }
    }

}
