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
import java.util.Arrays;
import java.util.Optional;

import static java.util.stream.Collectors.joining;
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
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = peopleRepository.findById(-1L);

        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canGetCount() {
        long startCount = peopleRepository.count();
        peopleRepository.save(new Person(
                "John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        ));
        peopleRepository.save(new Person(
                "Bobby", "Smith", ZonedDateTime.of(
                1982, 9, 13, 15, 15, 0, 0, ZoneId.of("-8"))
        ));
        long endCount = peopleRepository.count();

        assertThat(endCount).isEqualTo(startCount + 2);
    }

    @Test
    public void canDelete() {
        Person savedPerson = peopleRepository.save(new Person(
                "John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        long startCount = peopleRepository.count();
        peopleRepository.delete(savedPerson);
        long endCount = peopleRepository.count();

        assertThat(endCount).isEqualTo(startCount - 1);
    }

    @Test
    public void canDeleteMultiplePeople() {
        Person person1 = peopleRepository.save(new Person(
                "John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        ));
        Person person2 = peopleRepository.save(new Person(
                "Bobby", "Smith", ZonedDateTime.of(
                1982, 9, 13, 15, 15, 0, 0, ZoneId.of("-8"))
        ));
        long startCount = peopleRepository.count();
        peopleRepository.delete(person1, person2);
        long endCount = peopleRepository.count();

        assertThat(endCount).isEqualTo(startCount - 2);
    }

    @Test
    public void experiment() {
        Person person1 = new Person(10L, null, null, null);
        Person person2 = new Person(20L, null, null, null);
        Person person3 = new Person(30L, null, null, null);
        Person person4 = new Person(40L, null, null, null);
        Person person5 = new Person(50L, null, null, null);

        // DELETE FROM PERSON WHERE ID IN (10, 20, 30, 40, 50);

        Person[] peopleArray = Arrays.asList(person1, person2, person3, person4, person5).toArray(new Person[]{});
        String ids = Arrays.stream(peopleArray)
                .map(Person::getId)
                .map(String::valueOf)
                .collect(joining(", "));
        System.out.println(ids);
    }
}
