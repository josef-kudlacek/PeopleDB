package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Person;
import org.junit.jupiter.api.AfterEach;
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
    private PeopleRepository peopleRepository;

    @BeforeEach
    void setUp() throws SQLException {
        connection = DriverManager
                .getConnection("jdbc:h2:C:\\squirrel-sql-4.7.1\\db\\peopletest");
        connection.setAutoCommit(false);
        peopleRepository = new PeopleRepository(connection);
    }

    @AfterEach
    void tearDown() throws SQLException {
        if (connection != null) {
            connection.close();
        }
    }

    @Test
    public void canSaveOnePerson() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Person savedPerson = peopleRepository.save(john);

        assertThat(savedPerson.getId()).isGreaterThan(0);
    }

    @Test
    public void canSaveTwoPeople() {
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

    @Test
    public void canFindPersonById() {
        Person savedPerson = peopleRepository.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person foundPerson = peopleRepository.findById(savedPerson.getId());

        assertThat(foundPerson).isEqualTo(savedPerson);
    }
}
