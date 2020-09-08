package com.shinhan.bdu.sandbox.util;

import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.MapUtils;

public class CollectionUtil {

	public static void debugPrintMap(String alias, Map map) {
		MapUtils.debugPrint(System.out, alias, map);
	}
	
	public static void loggingPrintMap(String alias, Map map, org.slf4j.Logger logger) {
		map.forEach((key, value) -> { logger.info(key + " : " + value); });
	}

	public static Map<String, Map<String, String>> mergeMapsValueJava9(
			List<Map<String, Map<String, String>>> valueList) {
		return valueList.stream().flatMap(e -> e.entrySet().stream())
				.collect(Collectors.groupingBy(Map.Entry::getKey,
						Collectors.flatMapping(e -> e.getValue().entrySet().stream(),
								Collectors.<Map.Entry<String, String>, String, String>toMap(Map.Entry::getKey,
										Map.Entry::getValue))));
	}

	public static Map<String, Map<String, String>> mergeMapsValueJava8(
			List<Map<String, Map<String, String>>> valueList) {
		return valueList.stream().flatMap(e -> e.entrySet().stream())
				.collect(Collectors.groupingBy(Map.Entry::getKey,
						flatMapping(e -> e.getValue().entrySet().stream(),
								Collectors.<Map.Entry<String, String>, String, String>toMap(Map.Entry::getKey,
										Map.Entry::getValue))));
	}

	private static <T, U, A, R> Collector<T, ?, R> flatMapping(
			Function<? super T, ? extends Stream<? extends U>> mapper, Collector<? super U, A, R> downstream) {

		BiConsumer<A, ? super U> acc = downstream.accumulator();
		return Collector.of(downstream.supplier(), (a, t) -> {
			try (Stream<? extends U> s = mapper.apply(t)) {
				if (s != null)
					s.forEachOrdered(u -> acc.accept(a, u));
			}
		}, downstream.combiner(), downstream.finisher(),
				downstream.characteristics().toArray(new Collector.Characteristics[0]));
	}

	public static String getDay() {
		Timestamp timestamp = new Timestamp(System.currentTimeMillis());
		return ("" + timestamp).split(" ")[0];
	}
	
	public static List<String> getKeysWithMapList(List<Map> list) {
		return (List<String>) list.stream().flatMap(e -> e.entrySet().stream()).map(e -> ((Entry) e).getKey()).collect(Collectors.toList());
	}
	
	public static Map<String, Map<String, String>> mergeMapsValueLeftJoin(List<Map<String, Map<String, String>>> valueList, List<String> filterKey) {
		return valueList.stream().flatMap(e -> e.entrySet().stream())
				.filter(x -> filterKey.contains(x.getKey()))
				.collect(Collectors.groupingBy(Map.Entry::getKey,
						flatMapping(e -> e.getValue().entrySet().stream(),
									Collectors.<Map.Entry<String, String>, String, String>toMap(Map.Entry::getKey,
											Map.Entry::getValue))));
	}
	
	
}
