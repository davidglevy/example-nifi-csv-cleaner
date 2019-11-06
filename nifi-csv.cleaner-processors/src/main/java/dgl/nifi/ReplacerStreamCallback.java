package dgl.nifi;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;

//import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.processor.io.StreamCallback;

import com.google.common.base.Splitter;

public class ReplacerStreamCallback implements StreamCallback {

	private String fieldSeperator = "~^~";
	
	private String lineSeperator = "-|";
	
	@Override
	public void process(InputStream in, OutputStream out) throws IOException {
		try (InputStreamReader reader = new InputStreamReader(in);
				BufferedReader strReader = new BufferedReader(reader)) {
			
			// We use the guava splitter as it seems to be the most intelligent
			// with respect to retaining empty fields.
			Splitter splitter = Splitter.on(fieldSeperator);
			
			StringBuffer current = new StringBuffer();
			String latest = null;
			while ((latest = strReader.readLine()) != null) {
				
				// Check if we've found an empty line and not in
				// middle of a string.
				if (current.length() == 0 && latest.isEmpty()) {
					out.write("\n".getBytes());
					continue;
				}
				
				current.append(latest);
				
				if (latest.endsWith(lineSeperator)) {
					// Process this as a complete line.
					
					int lengthWeWant = current.length() - lineSeperator.length();
					Iterable<String> fields = splitter.split(current.substring(0, lengthWeWant));
					
					StringBuffer result = new StringBuffer();

					boolean first = true;
					for (String field : fields) {
						if (first) {
							first = false;
						} else {
							result.append(",");
						}
						result.append("\"");
						
						// Escape any quotes
						result.append(field.replaceAll("\"", "\"\""));
						
						result.append("\"");
					}
					result.append("\n");
					
					out.write(result.toString().getBytes());
					
					// Reset the String Buffer.
					current.delete(0, current.length());
				} else {
					// Add a newline to a non-terminated string.
					current.append("\n");
					
				}
			}
		}
	}

	public String getFieldSeperator() {
		return fieldSeperator;
	}

	public void setFieldSeperator(String fieldSeperator) {
		this.fieldSeperator = fieldSeperator;
	}

	public String getLineSeperator() {
		return lineSeperator;
	}

	public void setLineSeperator(String lineSeperator) {
		this.lineSeperator = lineSeperator;
	}

	
}
