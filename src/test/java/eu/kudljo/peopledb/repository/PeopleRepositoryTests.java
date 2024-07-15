package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Person;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private Connection connection;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager
                .getConnection("jdbc:h2:~/peopletest".replace("~", System.getProperty("user.home")));
    }

    @Test
    public void canSaveOnePerson() {
        PeopleRepository peopleRepository = new PeopleRepository(connection);
        Person john = new Person("John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Person savedPerson = peopleRepository.save(john);
        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
        PeopleRepository peopleRepository = new PeopleRepository(connection);
        Person john = new Person("John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Person bobby = new Person("Bobby", "Smith", ZonedDateTime.of(
                1982, 9, 13, 15, 15, 0, 0, ZoneId.of("-8"))
        );
        Person savedPerson1 = peopleRepository.save(john);
        Person savedPerson2 = peopleRepository.save(bobby);

        assertThat(savedPerson1.getId()).isNotEqualTo(savedPerson2.getId());
    }
}
