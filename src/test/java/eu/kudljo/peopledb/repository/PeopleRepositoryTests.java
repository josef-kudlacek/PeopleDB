package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.model.Address;
import eu.kudljo.peopledb.model.Person;
import eu.kudljo.peopledb.model.Region;
import org.junit.jupiter.api.*;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;
import java.util.Optional;

import static java.util.stream.Collectors.toSet;
import static org.assertj.core.api.Assertions.assertThat;

public class PeopleRepositoryTests {

    private static Connection connection;
    private static PeopleRepository peopleRepository;

    @BeforeAll
    static void setUp() throws SQLException {
        connection = DriverManager
                .getConnection("jdbc:h2:C:\\squirrel-sql-4.7.1\\db\\peopletest");
        connection.setAutoCommit(false);
        peopleRepository = new PeopleRepository(connection);
    }

    @AfterEach
    void reset() throws SQLException {
        connection.rollback();
    }

    @AfterAll
    static void tearDown() throws SQLException {
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
    public void canSavePersonWithHomeAddress() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala",
                "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = peopleRepository.save(john);
        assertThat(savedPerson.getHomeAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithBussAddress() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala",
                "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = peopleRepository.save(john);
        assertThat(savedPerson.getBusinessAddress().get().id()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithSpouse() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Person susan = new Person("SusanYYYY", "Smith", ZonedDateTime.of(
                1982, 9, 10, 23, 55, 59, 0, ZoneId.of("-6"))
        );
        john.setSpouse(susan);

        Person savedPerson = peopleRepository.save(john);
        assertThat(savedPerson.getSpouse().get().getId()).isGreaterThan(0);
    }

    @Test
    public void canSavePersonWithChildren() {
        Person john = new Person("John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(
                2010, 1, 1, 1, 0, 0, 0, ZoneId.of("-6"))
        ));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(
                2012, 1, 3, 1, 0, 0, 0, ZoneId.of("-6"))
        ));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(
                2014, 1, 5, 1, 0, 0, 0, ZoneId.of("-6"))
        ));

        Person savedPerson = peopleRepository.save(john);
        assertThat(savedPerson.getChildren().size()).isEqualTo(3);
        savedPerson.getChildren().stream()
                .map(Person::getId)
                .forEach(id -> assertThat(id).isGreaterThan(0));
//        connection.commit();
    }

    @Test
    public void canFindPersonById() {
        Person savedPerson = peopleRepository.save(new Person("test", "jackson", ZonedDateTime.now()));
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson).isEqualTo(savedPerson);
    }

    @Test
    public void canFindPersonByIdWithHomeAddress() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala",
                "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setHomeAddress(address);

        Person savedPerson = peopleRepository.save(john);
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getHomeAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithBusinessAddress() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Address address = new Address(null, "123 Beale St.", "Apt. 1A", "Wala Wala",
                "WA", "90210", "United States", "Fulton County", Region.WEST);
        john.setBusinessAddress(address);

        Person savedPerson = peopleRepository.save(john);
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getBusinessAddress().get().state()).isEqualTo("WA");
    }

    @Test
    public void canFindPersonByIdWithSpouse() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        Person susan = new Person("SusanYYYY", "Smith", ZonedDateTime.of(
                1982, 9, 10, 23, 55, 59, 0, ZoneId.of("-6"))
        );
        john.setSpouse(susan);

        Person savedPerson = peopleRepository.save(john);
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getSpouse().get().getId()).isGreaterThan(0);
    }

    @Test
    public void canFindPersonByIdWithChildren() {
        Person john = new Person("JohnZZZ", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6"))
        );
        john.addChild(new Person("Johnny", "Smith", ZonedDateTime.of(
                2010, 1, 1, 1, 0, 0, 0, ZoneId.of("-6"))
        ));
        john.addChild(new Person("Sarah", "Smith", ZonedDateTime.of(
                2012, 1, 3, 1, 0, 0, 0, ZoneId.of("-6"))
        ));
        john.addChild(new Person("Jenny", "Smith", ZonedDateTime.of(
                2014, 1, 5, 1, 0, 0, 0, ZoneId.of("-6"))
        ));

        Person savedPerson = peopleRepository.save(john);
        Person foundPerson = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(foundPerson.getChildren().stream()
                .map(Person::getFirstName)
                .collect(toSet()))
                .contains("Johnny", "Sarah", "Jenny");
    }

    @Test
    public void testPersonIdNotFound() {
        Optional<Person> foundPerson = peopleRepository.findById(-1L);

        assertThat(foundPerson).isEmpty();
    }

    @Test
    public void canFindAll() {
        peopleRepository.save(
                new Person("John", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John1", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John2", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John3", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John4", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John5", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John6", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John7", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        peopleRepository.save(
                new Person("John8", "Smith",
                        ZonedDateTime.of(1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );

        List<Person> people = peopleRepository.findAll();
        assertThat(people.size()).isGreaterThanOrEqualTo(9);
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
    public void canUpdate() {
        Person savedPerson = peopleRepository.save(new Person(
                "John", "Smith", ZonedDateTime.of(
                1980, 11, 15, 15, 15, 0, 0, ZoneId.of("-6")))
        );
        Person person1 = peopleRepository.findById(savedPerson.getId()).get();
        savedPerson.setSalary(new BigDecimal("73000.28"));
        peopleRepository.update(savedPerson);
        Person person2 = peopleRepository.findById(savedPerson.getId()).get();

        assertThat(person2.getSalary()).isNotEqualTo(person1.getSalary());
    }

    @Test
    @Disabled
    public void loadData() throws IOException, SQLException {
        Files.lines(Path.of("C:\\Users\\josep\\IdeaProjects\\Udemy\\Files\\Hr5m.csv"))
                .skip(1)
                .map(lines -> lines.split(","))
                .map(array -> {
                    LocalDate dob = LocalDate.parse(array[10], DateTimeFormatter.ofPattern("M/d/yyyy"));
                    LocalTime tob = LocalTime.parse(array[11], DateTimeFormatter.ofPattern("hh:mm:ss a", Locale.ENGLISH));
                    LocalDateTime dtob = LocalDateTime.of(dob, tob);
                    ZonedDateTime zdtob = ZonedDateTime.of(dtob, ZoneId.of("+0"));
                    Person person = new Person(array[2], array[4], zdtob);
                    person.setSalary(new BigDecimal(array[25]));
                    person.setEmail(array[6]);
                    return person;
                })
                .forEach(peopleRepository::save);

        connection.commit();
    }
}
