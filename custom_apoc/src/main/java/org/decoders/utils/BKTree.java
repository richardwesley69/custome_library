package org.decoders.utils;

import org.apache.commons.text.similarity.LevenshteinDistance;

import java.util.*;

public class BKTree {

	private static final LevenshteinDistance LEVENSHTEIN = LevenshteinDistance.getDefaultInstance();

	// BKTreeNode stores the value (name), unique ID, and child nodes (edges)
	static class BKTreeNode {
		String value;         // Name
		String uniqueId;      // Unique ID
		Map<Integer, BKTreeNode> edges = new HashMap<>();

		BKTreeNode(String value, String uniqueId) {
			this.value = value;
			this.uniqueId = uniqueId;
		}
	}

	private BKTreeNode root;

	// Method to insert a name and uniqueId into the BKTree
	public void insert(String name, String uniqueId) {
		if (root == null) {
			root = new BKTreeNode(name, uniqueId);
		} else {
			insertRecursive(root, name, uniqueId);
		}
	}

	private void insertRecursive(BKTreeNode node, String name, String uniqueId) {
		BKTreeNode currentNode = node;
		while (true) {
			int distance = LEVENSHTEIN.apply(currentNode.value, name);
			BKTreeNode child = currentNode.edges.get(distance);
			if (child == null) {
				currentNode.edges.put(distance, new BKTreeNode(name, uniqueId));
				break;
			} else {
				currentNode = child;
			}
		}
	}

	// Method to find names that match a given percentage or higher
	public List<String> findNamesBySimilarity(String queriedName, double percentageThreshold) {
		List<String> matchingNames = new ArrayList<>();
		if (root != null) {
			searchRecursive(root, queriedName, percentageThreshold, matchingNames);
		}
		return matchingNames;
	}

	private void searchRecursive(BKTreeNode node, String queriedName, double percentageThreshold, List<String> matchingNames) {
		int distance = LEVENSHTEIN.apply(node.value, queriedName);
		double similarityPercentage = calculateSimilarityPercentage(distance, node.value.length(), queriedName.length());

		if (similarityPercentage >= percentageThreshold) {
			matchingNames.add(node.uniqueId + ": " + node.value);
		}

		int range = (int) Math.ceil((1 - (percentageThreshold / 100)) * Math.max(node.value.length(), queriedName.length()));

		for (Map.Entry<Integer, BKTreeNode> entry : node.edges.entrySet()) {
			int childDistance = entry.getKey();
			if (Math.abs(distance - childDistance) <= range) {
				searchRecursive(entry.getValue(), queriedName, percentageThreshold, matchingNames);
			}
		}
	}

	private double calculateSimilarityPercentage(int distance, int length1, int length2) {
		int maxLength = Math.max(length1, length2);
		return (1 - ((double) distance / maxLength)) * 100;
	}

	private String getTreeStructureRecursive(BKTreeNode node, String prefix) {
		if (node == null) return "";
		StringBuilder treeContent = new StringBuilder();
		treeContent.append(prefix)
			.append("Name: ")
			.append(node.value)
			.append(", UniqueId: ")
			.append(node.uniqueId)
			.append("\n");

		for (BKTreeNode child : node.edges.values()) {
			treeContent.append(getTreeStructureRecursive(child, prefix + "  "));
		}
		return treeContent.toString();
	}
}
