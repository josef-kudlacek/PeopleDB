package eu.kudljo.peopledb.repository;

import eu.kudljo.peopledb.annotation.Id;
import eu.kudljo.peopledb.annotation.MultiSQL;
import eu.kudljo.peopledb.annotation.SQL;
import eu.kudljo.peopledb.model.CrudOperation;

import java.sql.*;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

abstract class CRUDRepository<T> {
    protected Connection connection;

    public CRUDRepository(Connection connection) {
        this.connection = connection;
    }

    public T save(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.SAVE, this::getSaveSql), PreparedStatement.RETURN_GENERATED_KEYS);
            mapForSave(entity, preparedStatement);
            int recordsAffected = preparedStatement.executeUpdate();
            ResultSet resultSet = preparedStatement.getGeneratedKeys();
            while (resultSet.next()) {
                long id = resultSet.getLong(1);
                setIdByAnnotation(id, entity);
                postSave(entity, id);
//                System.out.println(entity);
            }
//            System.out.printf("Records affected: %d%n", recordsAffected);
        } catch (SQLException e) {
            e.printStackTrace();
        }
        return entity;
    }

    public Optional<T> findById(Long id) {
        T entity = null;

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.FIND_BY_ID, this::getFindByIdSql));
            preparedStatement.setLong(1, id);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entity = extractEntityFromResultSet(resultSet);
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return Optional.ofNullable(entity);
    }

    public List<T> findAll() {
        List<T> entities = new ArrayList<>();

        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    getSQLByAnnotation(CrudOperation.FIND_ALL, this::getFindAllSql),
                    ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                entities.add(extractEntityFromResultSet(resultSet));
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return entities;
    }

    public long count() {
        long count = 0;
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.COUNT, this::getCountSql));
            ResultSet resultSet = preparedStatement.executeQuery();
            if (resultSet.next()) {
                count = resultSet.getLong("COUNT");
            }
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return count;
    }

    public void delete(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(getSQLByAnnotation(CrudOperation.DELETE_BY_ID, this::getDeleteSql));
            preparedStatement.setLong(1, getIdByAnnotation(entity));
            int affectedRecordCount = preparedStatement.executeUpdate();
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public void delete(T... entities) {
        try {
            Statement statement = connection.createStatement();
            String ids = Arrays.stream(entities)
                    .map(this::getIdByAnnotation)
                    .map(String::valueOf)
                    .collect(joining(", "));
            int affectedRecordCount = statement.executeUpdate(getSQLByAnnotation(CrudOperation.DELETE_BY_IDS, this::getDeleteInSql).replace(":ids", ids));
            System.out.println(affectedRecordCount);
        } catch (SQLException e) {
            e.printStackTrace();
        }

    }

    public void update(T entity) {
        try {
            PreparedStatement preparedStatement = connection.prepareStatement(
                    getSQLByAnnotation(CrudOperation.UPDATE, this::GetUpdateSql)
            );
            mapForUpdate(entity, preparedStatement);
            preparedStatement.executeUpdate();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    protected String GetUpdateSql() {
        throw new RuntimeException("SQL not defined");
    }

    /**
     *
     * @return Should return a SQL String like:
     * "DELETE FROM PEOPLE WHERE ID IN (:ids)"
     * Be sure to include the '(:ids)' named parameter & call it 'ids'
     */
    protected String getDeleteInSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getDeleteSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getCountSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getFindAllSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected String getSaveSql() {
        throw new RuntimeException("SQL not defined");
    }

    /**
     *
     * @return Returns String that represents the SQL needed to retrieve one entity.
     * The SQL must contain one SQL parameter, i.e. "?", that will bind to the entity's ID.
     */
    protected String getFindByIdSql() {
        throw new RuntimeException("SQL not defined");
    }

    protected void postSave(T entity, long id) { }

    abstract void mapForSave(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract void mapForUpdate(T entity, PreparedStatement preparedStatement) throws SQLException;

    abstract T extractEntityFromResultSet(ResultSet resultSet) throws SQLException;

    private String getSQLByAnnotation(CrudOperation operationType, Supplier<String> sqlGetter) {
        Stream<SQL> multiSqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(MultiSQL.class))
                .map(method -> method.getAnnotation(MultiSQL.class))
                .flatMap(multiSQL -> Arrays.stream(multiSQL.value()));

        Stream<SQL> sqlStream = Arrays.stream(this.getClass().getDeclaredMethods())
                .filter(method -> method.isAnnotationPresent(SQL.class))
                .map(method -> method.getAnnotation(SQL.class));

        return Stream.concat(multiSqlStream, sqlStream)
                .filter(annotation -> annotation.operationType().equals(operationType))
                .map(SQL::value)
                .findFirst()
                .orElseGet(sqlGetter);
    }

    private void setIdByAnnotation(Long id, T entity) {
        Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .forEach(field -> {
                    field.setAccessible(true);
                    try {
                        field.set(entity, id);
                    } catch (IllegalAccessException e) {
                        throw new RuntimeException("Unable to set value to field 'Id'");
                    }
                });
    }

    private Long getIdByAnnotation(T entity) {
        return Arrays.stream(entity.getClass().getDeclaredFields())
                .filter(field -> field.isAnnotationPresent(Id.class))
                .mapToLong(field -> {
                    field.setAccessible(true);
                    Long id = null;
                    try {
                        id = (long) field.get(entity);
                    } catch (IllegalAccessException e) {
                        e.printStackTrace();
                    }
                    return id;
                })
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Field with annotation 'Id' was not found"));
    }
}
