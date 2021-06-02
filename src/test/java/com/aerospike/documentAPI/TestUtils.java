package com.aerospike.documentAPI;

import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class TestUtils {

    /**
     * Determine whether two JSON objects are equivalent -
     * by serializing maps and list in a specific way and then testing serializations to see if they are equal.
     */
    public static boolean jsonEquals(Object object1, Object object2) {
        if ((object1 instanceof Map) && (object2 instanceof Map)) {
            return mapEquals((Map)object1,(Map)object2);
        } else if ((object1 instanceof List) && (object2 instanceof List)) {
            return listEquals((List)object1,(List)object2);
        } else {
            return object1.toString().equals(object2.toString());
        }
    }

    /**
     * Verify jsonEquals functionality.
     */
    @Test
    public void testMapEquals() {
        HashMap<String, Object> map1 = new HashMap<>();
        HashMap<String, Object> map2 = new HashMap<>();

        map1.put("key1",1);
        map1.put("key2",2);
        map1.put("key3",3);
        map2.put("key3",3);
        map2.put("key1",1);
        map2.put("key2",2);
        Assert.assertTrue(TestUtils.jsonEquals(map1, map2));
    }

    /**
     * Generic JSON to string method.
     */
    private static String jsonToString(Object o) {
        if (o instanceof Map) {
            return jsonMapToString((Map<String, Object>)o);
        } else if (o instanceof List) {
            return jsonListToString((List)o);
        } else if (o instanceof String) {
            return String.format("\"%s\"", o);
        } else {
            return o.toString();
        }
    }

    /**
     * Serialization of JSON map.
     */
    private static String jsonMapToString(Map<String, Object> map) {
        List<String> orderedKeys = new ArrayList(map.keySet());
        Collections.sort(orderedKeys);
        Iterator<String> keyIterator = orderedKeys.iterator();
        List<String> stringRepresentations = new Vector<>();

        while (keyIterator.hasNext()) {
            String key = keyIterator.next();
            String entry = String.format("\"%s\":%s", key, jsonToString(map.get(key)));
            stringRepresentations.add(entry);
        }
        return "{" + String.join(",", stringRepresentations) + "}";
    }

    /**
     * Serialization of JSON list.
     */
    private static String jsonListToString(List list) {
        Iterator listIterator = list.listIterator();
        List<String> stringRepresentations = new Vector<>();

        while (listIterator.hasNext()) {
            stringRepresentations.add(jsonToString(listIterator.next()));
        }
        return "[" + String.join(",", stringRepresentations) + "]";
    }

    /**
     * Determine whether two JSON maps are equivalent.
     */
    private static boolean mapEquals(Map map1, Map map2) {
        return jsonMapToString(map1).equals(jsonMapToString(map2));
    }

    /**
     * Determine whether two JSON lists are equivalent.
     */
    private static boolean listEquals(List list1, List list2) {
        return jsonListToString(list1).equals(jsonListToString(list2));
    }
}
