package de.hpi.ddm;

// Java program to print all
// permutations using Johnson
// and Trotter algorithm.

import org.apache.commons.lang3.tuple.Pair;

import java.util.Collection;
import java.util.HashSet;
import java.util.Queue;

import static de.hpi.ddm.Utils.permutationsForPasswordCracking;

public class PermutationGenerator{

    private final static boolean LEFT_TO_RIGHT = true;
    private final static boolean RIGHT_TO_LEFT = false;

    private boolean[] dir;
    private int currentTotalPermutations;
    private int permutationCount = 1;
    private int[] currentPermutation;
    private Queue<String> combinations;
    private String currentCombination;

    public PermutationGenerator(Queue<String> combinations) {
        this.combinations = combinations;
        nextCombination();
    }

    // Utility functions for
    // finding the position
    // of largest mobile
    // integer in a[].
    private int searchArr(int a[], int n,
                                int mobile)
    {
        for (int i = 0; i < n; i++)

            if (a[i] == mobile)
                return i + 1;

        return 0;
    }

    // To carry out step 1
    // of the algorithm i.e.
    // to find the largest
    // mobile integer.
    private int getMobile(int a[],
                                boolean dir[], int n)
    {
        int mobile_prev = 0, mobile = 0;

        for (int i = 0; i < n; i++)
        {
            // direction 0 represents
            // RIGHT TO LEFT.
            if (dir[a[i] - 1] == RIGHT_TO_LEFT &&
                    i != 0)
            {
                if (a[i] > a[i - 1] &&
                        a[i] > mobile_prev)
                {
                    mobile = a[i];
                    mobile_prev = mobile;
                }
            }

            // direction 1 represents
            // LEFT TO RIGHT.
            if (dir[a[i] - 1] == LEFT_TO_RIGHT &&
                    i != n - 1)
            {
                if (a[i] > a[i + 1] &&
                        a[i] > mobile_prev)
                {
                    mobile = a[i];
                    mobile_prev = mobile;
                }
            }
        }

        if (mobile == 0 && mobile_prev == 0)
            return 0;
        else
            return mobile;
    }

    // Prints a single
    // permutation
    private void calculateOnePermutation(int a[], boolean dir[],
                                   int n)
    {
        if (permutationCount == 0) return;
        int mobile = getMobile(a, dir, n);
        int pos = searchArr(a, n, mobile);

        // swapping the elements
        // according to the
        // direction i.e. dir[].
        if (dir[a[pos - 1] - 1] == RIGHT_TO_LEFT)
        {
            int temp = a[pos - 1];
            a[pos - 1] = a[pos - 2];
            a[pos - 2] = temp;
        }
        else if (dir[a[pos - 1] - 1] == LEFT_TO_RIGHT)
        {
            int temp = a[pos];
            a[pos] = a[pos - 1];
            a[pos - 1] = temp;
        }

        // changing the directions
        // for elements greater
        // than largest mobile integer.
        for (int i = 0; i < n; i++)
        {
            if (a[i] > mobile)
            {
                if (dir[a[i] - 1] == LEFT_TO_RIGHT)
                    dir[a[i] - 1] = RIGHT_TO_LEFT;

                else if (dir[a[i] - 1] == RIGHT_TO_LEFT)
                    dir[a[i] - 1] = LEFT_TO_RIGHT;
            }
        }
    }

    public Collection<String> getNextBatch(int chunkSize) {
        HashSet<String> permutations = new HashSet<>(chunkSize);

        while(permutations.size() < chunkSize) {
            for (; permutationCount < Math.min(permutationCount + chunkSize - permutations.size(), currentTotalPermutations); permutationCount++) {
                calculateOnePermutation(currentPermutation, dir, currentPermutation.length);
                permutations.add(map(currentPermutation));
            }

            if(currentTotalPermutations == permutationCount && !nextCombination())
                break;
        }

        return permutations;
    }

    private boolean nextCombination() {
        if(combinations.isEmpty()) return false;

        currentCombination = combinations.poll();
        currentPermutation = new int[currentCombination.length()];
        dir = new boolean[currentPermutation.length];
        for (int iChar = 0; iChar < currentPermutation.length; iChar++) {
            currentPermutation[iChar] = iChar + 1;
            dir[iChar] = RIGHT_TO_LEFT;
        }
        currentTotalPermutations = (int) fact(currentPermutation.length);
        permutationCount = 0;
        return true;
    }

    private String map(int[] order) {
        char[] mappedChars = new char[order.length];
        for (int iChar = 0; iChar < mappedChars.length; iChar++)
            mappedChars[iChar] = currentCombination.charAt(order[iChar] - 1);
        return new String(mappedChars);
    }

    // To end the algorithm
    // for efficiency it ends
    // at the factorial of n
    // because number of
    // permutations possible
    // is just n!.
    public static long fact(int n)
    {
        long res = 1;

        for (int i = 1; i <= n; i++)
            res = res * i;
        return res;
    }

}

// This code is contributed by Sagar Shukla
