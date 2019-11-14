package de.hpi.ddm.actors;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;

import akka.actor.AbstractLoggingActor;
import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent.CurrentClusterState;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.Member;
import akka.cluster.MemberStatus;
import de.hpi.ddm.MasterSystem;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.RequiredArgsConstructor;
import org.apache.commons.lang3.tuple.Pair;

public class Worker extends AbstractLoggingActor {

	////////////////////////
	// Actor Construction //
	////////////////////////
	
	public static final String DEFAULT_NAME = "worker";

	public static Props props() {
		return Props.create(Worker.class);
	}

	public Worker() {
		this.cluster = Cluster.get(this.context().system());
	}
	
	////////////////////
	// Actor Messages //
	////////////////////

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class CompareMessage implements Serializable {
		private static final long serialVersionUID = 3303081601659723997L;
		private int offset;
		private int length;
		private String hash;
		private String occurringCharacters;
		private List<String> hashCache;

		public boolean hasHashCache() { return getHashCache() != null; }
	}

	@Data @AllArgsConstructor @NoArgsConstructor
	public static class CompareResult implements Serializable {
		private static final long serialVersionUID = 1294419813760526676L;
		private String matchingPermutation;
	}

	/////////////////
	// Actor State //
	/////////////////

	private Member masterSystem;
	private final Cluster cluster;
	
	/////////////////////
	// Actor Lifecycle //
	/////////////////////

	@Override
	public void preStart() {
		Reaper.watchWithDefaultReaper(this);
		
		this.cluster.subscribe(this.self(), MemberUp.class, MemberRemoved.class);
	}

	@Override
	public void postStop() {
		this.cluster.unsubscribe(this.self());
	}

	////////////////////
	// Actor Behavior //
	////////////////////

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(CurrentClusterState.class, this::handle)
				.match(MemberUp.class, this::handle)
				.match(MemberRemoved.class, this::handle)
				.match(CompareMessage.class, this::handle)
				.matchAny(object -> this.log().info("Received unknown message: \"{}\"", object.toString()))
				.build();
	}

	private void handle(CurrentClusterState message) {
		message.getMembers().forEach(member -> {
			if (member.status().equals(MemberStatus.up()))
				this.register(member);
		});
	}

	private void handle(MemberUp message) {
		this.register(message.member());
	}

	private void register(Member member) {
		if ((this.masterSystem == null) && member.hasRole(MasterSystem.MASTER_ROLE)) {
			this.masterSystem = member;
			
			this.getContext()
				.actorSelection(member.address() + "/user/" + Master.DEFAULT_NAME)
				.tell(new Master.RegistrationMessage(), this.self());
		}
	}
	
	private void handle(MemberRemoved message) {
		if (this.masterSystem.equals(message.member()))
			this.self().tell(PoisonPill.getInstance(), ActorRef.noSender());
	}

	private void handle(CompareMessage compareMessage) {
		List<String> hashCache = compareMessage.hasHashCache() ?
				compareMessage.getHashCache() : Arrays.asList(new String[compareMessage.getLength()]);

		Pair<Boolean, String> result = findPermutationForHash(
				compareMessage.getOccurringCharacters().toCharArray(),
				hashCache,
				compareMessage.getOffset(),
				compareMessage.getHash());
		if (result.getLeft())
			this.getContext()
					.actorSelection(this.masterSystem.address() + "/user/" + Master.DEFAULT_NAME)
					.tell(new Master.StoreHashesMessage(compareMessage.getOffset(), compareMessage.getOccurringCharacters(), hashCache), this.self());
		this.sender().tell(new CompareResult(result.getRight()), this.self());
	}

	private Pair<Boolean, String> findPermutationForHash(char[] chars, List<String> hashCache, int offset, String targetHash) {
		ArrayList<String> permutations = new ArrayList<>();
		heapPermutation(chars, chars.length, permutations);
		boolean updatedCache = false;
		String matchingPermutation = null;
		for (int iEntry = 0; iEntry < hashCache.size(); iEntry++) {
			if (hashCache.get(iEntry) == null) {
				hashCache.set(iEntry, hash(permutations.get(offset + iEntry)));
				updatedCache = true;
			}
			if (hashCache.get(iEntry).equals(targetHash)) {
				matchingPermutation = permutations.get(offset + iEntry);
				break;
			}
		}
		return Pair.of(updatedCache, matchingPermutation);
	}

	private HashMap<String, String> hash(List<String> permutations) {
		HashMap<String, String> hashes = new HashMap<>();
		permutations.forEach(code -> hashes.put(code, hash(code)));
		return hashes;
	}

	private String hash(String line) {
		try {
			MessageDigest digest = MessageDigest.getInstance("SHA-256");
			byte[] hashedBytes = digest.digest(String.valueOf(line).getBytes("UTF-8"));
			
			StringBuffer stringBuffer = new StringBuffer();
			for (int i = 0; i < hashedBytes.length; i++) {
				stringBuffer.append(Integer.toString((hashedBytes[i] & 0xff) + 0x100, 16).substring(1));
			}
			return stringBuffer.toString();
		}
		catch (NoSuchAlgorithmException | UnsupportedEncodingException e) {
			throw new RuntimeException(e.getMessage());
		}
	}
	
	// Generating all permutations of an array using Heap's Algorithm
	// https://en.wikipedia.org/wiki/Heap's_algorithm
	// https://www.geeksforgeeks.org/heaps-algorithm-for-generating-permutations/
	private void heapPermutation(char[] a, int size, List<String> l) {
		// If size is 1, store the obtained permutation
		if (size == 1)
			l.add(new String(a));

		for (int i = 0; i < size; i++) {
			heapPermutation(a, size - 1, l);

			// If size is odd, swap first and last element
			if (size % 2 == 1) {
				char temp = a[0];
				a[0] = a[size - 1];
				a[size - 1] = temp;
			}

			// If size is even, swap i-th and last element
			else {
				char temp = a[i];
				a[i] = a[size - 1];
				a[size - 1] = temp;
			}
		}
	}
}