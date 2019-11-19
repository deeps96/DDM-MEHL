package de.hpi.ddm;

// Java program to print all
// permutations using Johnson
// and Trotter algorithm.
import scala.Char;

import java.util.*;
import java.lang.*;
import java.util.stream.Stream;

public class PermutationGenerator{

    private final static boolean LEFT_TO_RIGHT = true;
    private final static boolean RIGHT_TO_LEFT = false;

    private boolean[] dir;
    private int currentTotalPermutations;
    private int permutationCount = 1;
    private int[] currentPermutation;
    private Queue<String> combinations;

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

    public List<String> getNextBatch(int chunkSize) {
        List<String> permutations = new ArrayList<>(chunkSize);

        while(permutations.size() < chunkSize && (!combinations.isEmpty() || permutationCount < currentTotalPermutations)) {
            for (; permutationCount < Math.min(permutationCount + chunkSize - permutations.size(), currentTotalPermutations); permutationCount++) {
                calculateOnePermutation(currentPermutation, dir, currentPermutation.length);
                char[] permutationChars = new char[currentPermutation.length];
                for(int i = 0; i < currentPermutation.length; i++) {
                    permutationChars[i] = (char) currentPermutation[i];
                }
                permutations.add(new String(permutationChars));
            }

            if(currentTotalPermutations == permutationCount) {
                nextCombination();
            }
        }

        return permutations;
    }

    private void nextCombination() {
        if(combinations.isEmpty()) {
            return;
        }

        currentPermutation = combinations.poll().chars().toArray();
        currentTotalPermutations = fact(currentPermutation.length);
        permutationCount = 0;
        dir = new boolean[Arrays.stream(currentPermutation).max().getAsInt()];
    }

    // To end the algorithm
    // for efficiency it ends
    // at the factorial of n
    // because number of
    // permutations possible
    // is just n!.
    public static int fact(int n)
    {
        int res = 1;

        for (int i = 1; i <= n; i++)
            res = res * i;
        return res;
    }

    public static void main(String[] args) {
        PermutationGenerator generator = new PermutationGenerator(new LinkedList<>(Arrays.asList("1234", "abcd")));
        for(int i = 0; i < 2; i++) {
            List<String> perms = generator.getNextBatch(10);
            System.out.println(perms);
        }
    }
}

// This code is contributed by Sagar Shukla
