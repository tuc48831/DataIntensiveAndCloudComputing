package CIS5517.wordCount;

import static org.junit.Assert.*;

import org.junit.Test;

public class jsonMapperTest {

	@Test
	public void testGetIdAndTimestamp() {
		String inputFilePath = "112233_201812111010.txt";
		jsonMapper temp = new jsonMapper();
		String[] strings = temp.getIdAndTimestamp(inputFilePath);
		assertEquals(strings[0], "112233");
		assertEquals(strings[1], "201812111010");
	}

}
