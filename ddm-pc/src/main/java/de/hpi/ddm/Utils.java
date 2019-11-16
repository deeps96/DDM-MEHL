package de.hpi.ddm;

import java.io.UnsupportedEncodingException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.*;
import java.util.stream.Collectors;

public class Utils {

    public static String hash(String line) {
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
    private static void heapPermutation(char[] a, int size, Set<String> l) {
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

    //https://hmkcode.com/calculate-find-all-possible-combinations-of-an-array-using-java/
    private static void combinationWithoutRepetition(char[] elements, int k, Set<String> combinations){

        // get the length of the array
        // e.g. for {'A','B','C','D'} => N = 4
        int N = elements.length;

        if(k > N){
            combinations.add(new String(elements));
            return;
        }

        // init combination index array
        int[] pointers = new int[k];


        int r = 0; // index for combination array
        int i = 0; // index for elements array

        while(r >= 0){

            // forward step if i < (N + (r-K))
            if(i <= (N + (r - k))){
                pointers[r] = i;

                // if combination array is full print and increment i;
                if(r == k-1){
                    char[] combination = new char[k];
                    for (int iChar = 0; iChar < k; iChar++)
                        combination[iChar] = elements[pointers[iChar]];
                    combinations.add(new String(combination));
                    i++;
                }
                else{
                    // if combination is not full yet, select next element
                    i = pointers[r]+1;
                    r++;
                }
            }

            // backward step
            else{
                r--;
                if(r >= 0)
                    i = pointers[r]+1;

            }
        }
    }

    // https://www.techiedelight.com/find-distinct-combinations-given-length-repetition-allowed/
    private static void combinationWithRepetition(char[] A, List<Character> out, int k, int i, int n, Set<String> combinations)
    {
        // base case: if combination size is k, print it
        if (out.size() == k)
        {
            combinations.add(out.stream()
                    .map(String::valueOf)
                    .collect(Collectors.joining()));
            return;
        }

        // start from previous element in the current combination
        // till last element
        for (int j = i; j < n; j++)
        {
            // add current element A[j] to the solution and recur with
            // same index j (as repeated elements are allowed in combinations)
            out.add(A[j]);
            combinationWithRepetition(A, out, k, j, n, combinations);

            // backtrack - remove current element from solution
            out.remove(out.size() - 1);
        }
    }

    public static void permutation(char[] A, int k, Set<String> permutations) {
        Set<String> combinations = new HashSet<>();
        if (k <= A.length) {
            combinationWithoutRepetition(A, k, combinations);
        } else {
            List<Character> out = new ArrayList<>();
            combinationWithRepetition(A, out, k, 0, A.length, combinations);
        }
        combinations.forEach(combination ->
                heapPermutation(combination.toCharArray(), k, permutations));
    }
}
