package com.samples.test;

import static org.junit.Assert.fail;
import junit.framework.Assert;

import org.junit.Test;

public class TestRegex {

	@Test
	public void test() {
		String text = "he^llo!\"";
	
	    Assert.assertEquals(text.replaceAll("[^0-9a-zA-Z]", ""), "hello");				
	}

}
