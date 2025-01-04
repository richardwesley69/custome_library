package org.decoders.utils;

import org.neo4j.driver.*;
import org.neo4j.procedure.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

public class CustomProcedure {

	private static final BKTree bkTree = new BKTree();

	// Method to insert data into the BKTree
	@Procedure(name = "custom.insertBKTree", mode = Mode.WRITE)
	@Description("Inserts a name and uniqueId into the BKTree")
	public void insertIntoBKTree() {
		String uri = "bolt://38.242.220.73:7687";
		String username = "neo4j";
		String password = "password";
		// Create the Neo4j driver
		Driver driver = GraphDatabase.driver(uri, AuthTokens.basic(username, password));

		// Atomic integer for counting inserted records
		AtomicInteger totalInserted = new AtomicInteger(0); // Thread-safe counter
		// Create a session
		long startTime = System.currentTimeMillis();

		try (Session session = driver.session()) {
			session.executeRead(tx -> {
				String query = "MATCH (n:NameValue {type:'Person'}) " +
					"RETURN n.recordId AS id, n.fullName AS SDNName";

				Result result = tx.run(query);
				result.list().stream()
					.map(record -> new AbstractMap.SimpleEntry<>(
						record.get("SDNName").asString(),  // Name
						record.get("id").asString()        // ID
					))
					.forEach(entry -> {
						// Increment the counter for inserted records
						int currentCount = totalInserted.incrementAndGet();
						bkTree.insert(entry.getKey(),entry.getValue());
						// Log progress every 1000 records
						if (currentCount % 1000 == 0) {
							System.out.println("Inserted " + currentCount + " records...");
						}
					});
				return null;
			});
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			driver.close();
		}

		long endTime = System.currentTimeMillis();
		System.out.println("Total records inserted: " + totalInserted.get());
		System.out.println("PROCESS TAKEN : " + (endTime - startTime) + "ms");
	}

	// Method to fetch names from the BKTree by similarity percentage
	@Procedure(name = "custom.fetchBKTree", mode = Mode.READ)
	@Description("Fetches names from the BKTree that match a queried name based on similarity percentage")
	public Stream<NameResult> fetch(@Name("queriedName") String queriedName, @Name("percentageThreshold") double percentageThreshold) {
		List<String> results = bkTree.findNamesBySimilarity(queriedName, percentageThreshold);
		return results.stream().map(NameResult::new);
	}

	// Helper class to represent the result of a query
	public static class NameResult {
		public String result;

		public NameResult(String result) {
			this.result = result;
		}
	}
}
