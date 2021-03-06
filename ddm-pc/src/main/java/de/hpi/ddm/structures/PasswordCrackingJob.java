package de.hpi.ddm.structures;

import de.hpi.ddm.PermutationGenerator;
import lombok.AccessLevel;
import lombok.Data;
import lombok.Getter;
import lombok.Setter;

import java.util.LinkedList;
import java.util.UUID;
import java.util.stream.Collectors;

@Data
public class PasswordCrackingJob {

    @Getter(AccessLevel.NONE) @Setter(AccessLevel.NONE)
    private boolean readyToCrackPassword = false;
    private boolean started = false;
    private int passwordLength;
    private int unresolvedHintCount;
    private LinkedList<Character> remainingChars;
    private LinkedList<String> hints;
    private LinkedList<String> permutations;
    private PermutationGenerator permutationGenerator;
    private String crackedPassword = null;
    private String hash;
    private String id;
    private long numberOfPermutationsPerHint;

    public PasswordCrackingJob(UUID id, String occurringCharacters, String hash, LinkedList<String> hints, int passwordLength) {
        setId(id.toString());
        setHash(hash);
        setUnresolvedHintCount(hints.size());
        setRemainingChars(occurringCharacters
                .chars()
                .mapToObj(c -> (char) c)
                .collect(Collectors.toCollection(LinkedList::new)));
        setHints(hints);
        setPasswordLength(passwordLength);
        setNumberOfPermutationsPerHint(PermutationGenerator.fact(occurringCharacters.length() - 1));
    }

    public String getRemainingCharsAsString() {
        return getRemainingChars()
                .stream()
                .map(String::valueOf)
                .collect(Collectors.joining());
    }

    public boolean hasUnresolvedHints() { return getUnresolvedHintCount() > 0; }

    public void decrementUnresolvedHintCount() { setUnresolvedHintCount(getUnresolvedHintCount() - 1); }

    public boolean isSolved() { return getCrackedPassword() != null; }


    public boolean readyToCrackPassword() {
        if(getUnresolvedHintCount() == 0 || this.readyToCrackPassword) {
            return true;
        }

        return false;

//        long numberOfPasswordCombinations = (long) Math.pow(getRemainingChars().size(), getPasswordLength());
//        long numberOfUnsolvedHintCombinations = getUnresolvedHintCount() * getNumberOfPermutationsPerHint();
//
//        this.readyToCrackPassword = numberOfPasswordCombinations <= numberOfUnsolvedHintCombinations;
//        return this.readyToCrackPassword;
    }
}
