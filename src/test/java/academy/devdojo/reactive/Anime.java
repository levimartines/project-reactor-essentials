package academy.devdojo.reactive;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@ToString
@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class Anime {

    private String title;
    private String studio;
    private int episodes;

}
