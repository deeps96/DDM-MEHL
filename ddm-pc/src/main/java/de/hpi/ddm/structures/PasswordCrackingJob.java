package de.hpi.ddm.structures;

import lombok.Data;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

@Data
public class PasswordCrackingJob {

    private int unresolvedHintCount;
    private List<Character> remainingChars;
    private List<String> hints;
    private String crackedPassword;
    private String hash;
    private UUID id;

    public PasswordCrackingJob(UUID id, String occurringCharacters, String hash, List<String> hints) {
        setId(id);
        setHash(hash);
        setUnresolvedHintCount(hints.size());
        setRemainingChars(occurringCharacters
                .chars()
                .mapToObj(c -> (char) c)
                .collect(Collectors.toList()));
        setHints(hints);
    }

    public String getRemainingCharsAsString() {
        return getRemainingChars()
                .stream()
                .map(String::valueOf)
                .collect(Collectors.joining());
    }
}
