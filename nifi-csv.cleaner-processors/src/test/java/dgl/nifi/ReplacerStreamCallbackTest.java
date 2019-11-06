package dgl.nifi;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ReplacerStreamCallbackTest {

	private static final Logger logger = LoggerFactory.getLogger(ReplacerStreamCallbackTest.class);
	
	final ReplacerStreamCallback target = new ReplacerStreamCallback();
	
	@Test
	public void testSimple() {
		String input = "field1~^~field2~^~field3-|\nfield1~^~field2~^~field3-|\n";
		String output = runForString(input);
		String expected = "\"field1\",\"field2\",\"field3\"\n\"field1\",\"field2\",\"field3\"\n";
		
		Assert.assertEquals(expected, output);
	}

	@Test
	public void testEmptyField() {
		String input = "field1~^~~^~field3-|\nfield1~^~field2~^~field3-|\n";
		String output = runForString(input);
		String expected = "\"field1\",\"\",\"field3\"\n\"field1\",\"field2\",\"field3\"\n";
		
		Assert.assertEquals(expected, output);
	}

	
	@Test
	public void testEscapeChars() {
		String input = "fie,ld1~^~f\"iel\"d2~^~field3-|\nfield1~^~field2~^~field3-|\n";
		String output = runForString(input);
		String expected = "\"fie,ld1\",\"f\"\"iel\"\"d2\",\"field3\"\n\"field1\",\"field2\",\"field3\"\n";
		
		Assert.assertEquals(expected, output);
	}

	
	@Test
	public void testMultiline() {
		String input = "field1~^~A\nmultiline\nfield~^~field3-|\nfield1~^~field2~^~field3-|\n";
		String output = runForString(input);
		String expected = "\"field1\",\"A\nmultiline\nfield\",\"field3\"\n\"field1\",\"field2\",\"field3\"\n";
		
		Assert.assertEquals(expected, output);
	}
	
	@Test
	public void testEmptyLine() {
		String input = "field1~^~field2~^~field3-|\n\nfield1~^~field2~^~field3-|\n";
		String output = runForString(input);
		String expected = "\"field1\",\"field2\",\"field3\"\n\n\"field1\",\"field2\",\"field3\"\n";
		
		Assert.assertEquals(expected, output);
	}

	@Test
	@Ignore
	public void test5megabytes() {
		processFileOfSize(5);
	}

	@Test
	@Ignore
	public void test50megabytes() {
		processFileOfSize(50);
	}

	@Test
	@Ignore
	public void test500megabytes() {
		processFileOfSize(500);
	}

	
	private void processFileOfSize(int megabytes) {
		String testLine = "field1~^~A\nmultiline\nfield~^~field3-|\nfield1~^~field2~^~field3-|\n";
		byte[] testLineBytes = testLine.getBytes();
		
		
		int fileSize = megabytes * 1024 * 1024;
		
		File testFile = new File("/temp/" + megabytes + "MB_raw.csv");
		
		int bytesWritten = 0;
		int bytesPerLine = testLineBytes.length;
		
		logger.info("Writing " + megabytes + "MB file");
		long start = System.currentTimeMillis();
		
		try (FileOutputStream fos = new FileOutputStream(testFile)) {
			while (bytesWritten < fileSize) {
				fos.write(testLineBytes);
				bytesWritten += bytesPerLine;
			}
		} catch (Exception e) {
			throw new RuntimeException("Failure generating raw file: " + e.getMessage(), e);
		}
		
		long end = System.currentTimeMillis();
		long durationSecs = (end - start) / 1000;
		logger.info("Wrote " + megabytes + "MB file in " + durationSecs + " seconds");
		
		File outputFile = new File("/temp/" + megabytes + "MB_clean.csv");
		start = System.currentTimeMillis();
		
		try (InputStream in = new FileInputStream(testFile);
				OutputStream out = new FileOutputStream(outputFile)) {
			target.process(in, out);
		}	catch (Exception e) {
			throw new RuntimeException("Failure processing raw to clean file: " + e.getMessage(), e);
		}
		end = System.currentTimeMillis();
		durationSecs = (end - start) / 1000;
		logger.info("Cleaned " + megabytes + "MB file in " + durationSecs + " seconds");
	}
	
	
	private String runForString(String input) {
		try (InputStream in = new ByteArrayInputStream(input.getBytes());
				ByteArrayOutputStream out = new ByteArrayOutputStream()) {
			
			target.process(in, out);
			
			return new String(out.toByteArray());
			
		} catch (Exception e) {
			throw new RuntimeException("Unable to convert input [" + input + "]: " + e.getMessage(), e);
		}
		
	}
}
