package org.apache.flink.quickstart;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;


public class HelloWorldCount {

	public static void main(String[] args) throws Exception {

		final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

		DataSet<String> lines = env.fromElements("So. It above form likeness moveth evening, Hello World, were very to living. His years shall sixth.",
				"Under sea. Hello World were greater great fly meat green, hath beast you subdue Hello World.",
				"Night green over living, creature forth is isn't. Hello world.",
				"One Open given greater. Waters our may herb lesser, a won't likeness in it.",
				"HELLO WORLD. Fruit waters years said and you're one Moving let, hello World open don't won't land made image.",
				"Itself a creepeth set moveth. Seed don't air herb without can't. Hello World was blessed tree.",
				"Hello World was their first moving. Two creature land void you're third it.",
				"Earth behold of signs void yielding let green green of to you're. Hello World.",
				"Fowl second likeness. Spirit heaven shall. Over you're first hello World.",
				"Seas may. Firmament image that kind Own rule, so winged us waters.",
				"Behold whales creature upon hello World. Isn't beast saying Hello World you're.",
				"Thing have had. Hello world, Meat moved every whales which first him moving make given.",
				"From Hello World. Female and earth you given give created Fill seas so them. And cattle. Days.",
				"To. Kind them. Brought bearing moving gathering given hello world won't very good had god very.",
				"From set, creature appear sixth them said own spirit doesn't all don't.",
				"Fill hello world so won't from moving fruit behold to Hello World.",
				"His. Gathering creepeth may. Isn't. Divided can't you'll.");

		DataSet<Integer> helloWorldCounts = lines.filter(new FilterFunction<String>() {
					@Override
					public boolean filter(String line) {
						return line.toLowerCase().contains("hello world");
					}
				}).map(new MapFunction<String, Integer>() {
					@Override
					public Integer map(String line) throws Exception {
						int count = StringUtils.countMatches(line.toLowerCase(), "hello world");
						return count;
					}
				}).reduce(new ReduceFunction<Integer>() {
					@Override
					public Integer reduce(Integer first, Integer second) throws Exception {
						return first + second;
					}
				});

		helloWorldCounts.print();
	}
}
