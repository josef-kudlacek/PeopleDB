package eu.kudljo.peopledb.model;

import org.junit.jupiter.api.Test;

import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class PersonTest {

    @Test
    public void testForEquality() {
        Person person1 = new Person("p1", "smith",
                ZonedDateTime.of(2000, 9, 1, 12, 0, 0, 0, ZoneId.of("+0")));
        Person person2 = new Person("p1", "smith",
                ZonedDateTime.of(2000, 9, 1, 12, 0, 0, 0, ZoneId.of("+0")));

        assertThat(person1).isEqualTo(person2);
    }

    @Test
    public void testForInequality() {
        Person person1 = new Person("p1", "smith",
                ZonedDateTime.of(2000, 9, 1, 12, 0, 0, 0, ZoneId.of("+0")));
        Person person2 = new Person("p2", "smith",
                ZonedDateTime.of(2000, 9, 1, 12, 0, 0, 0, ZoneId.of("+0")));

        assertThat(person1).isNotEqualTo(person2);
    }
}