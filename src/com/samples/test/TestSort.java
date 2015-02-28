package com.samples.test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.TestCase;

public class TestSort extends TestCase {

	public void testSort(){
		List<String> list = new ArrayList<String>();
		list.add("For Love");
		list.add("What is");
		list.add("If this");
		list.add("Then that");
		list.add("For God");
		list.add("What *");
		list.add("Then What");
		list.add("What if");
		list.add("Then *");
		list.add("For *");
		Collections.sort(list);
		
		for (String string : list) {
			System.out.println(string);
		}
		
	}
}
