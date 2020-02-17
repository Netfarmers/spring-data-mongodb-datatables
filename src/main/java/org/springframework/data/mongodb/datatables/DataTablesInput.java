package org.springframework.data.mongodb.datatables;

import com.fasterxml.jackson.annotation.JsonIgnore;
import lombok.Data;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Optional.ofNullable;
import static java.util.stream.Collectors.toMap;

@Data
public class DataTablesInput {

    /**
     * Draw counter. This is used by DataTables to ensure that the Ajax returns from server-side
     * processing requests are drawn in sequence by DataTables (Ajax requests are asynchronous and
     * thus can return out of sequence). This is used as part of the draw return parameter (see
     * below).
     */
    @NotNull
    @Min(0)
    private int draw = 1;

    /**
     * Paging first record indicator. This is the start point in the current data set (0 index based -
     * i.e. 0 is the first record).
     */
    @NotNull
    @Min(0)
    private int start = 0;

    /**
     * Number of records that the table can display in the current draw. It is expected that the
     * number of records returned will be equal to this number, unless the server has fewer records to
     * return. Note that this can be -1 to indicate that all records should be returned (although that
     * negates any benefits of server-side processing!)
     */
    @NotNull
    @Min(-1)
    private int length = 10;

    /**
     * Global search parameter.
     */
    @NotNull
    private Search search;

    /**
     * Order parameter
     */
    @NotEmpty
    private List<Order> order;

    /**
     * Per-column search parameter
     */
    @NotEmpty
    private List<Column> columns;

    @JsonIgnore
    private Map<String, Column> columnMap;

    public void setColumns(List<Column> columns) {
        this.columns = columns;
        this.columnMap = columns.stream().collect(toMap(Column::getData, x -> x));
    }

    public Optional<Column> getColumn(String columnName) {
        if (columns.size() != columnMap.size()) {
            this.columnMap = columns.stream().collect(toMap(Column::getData, x -> x));
        }

        return ofNullable(columnMap.get(columnName));
    }

    @Data
    public static final class Column {

        /**
         * Column's data source
         *
         * @see <a href="http://datatables.net/reference/option/columns.data">http://datatables.net/reference/option/columns.data</a>
         */
        @NotBlank
        private String data;

        /**
         * Column's name
         *
         * @see <a href="http://datatables.net/reference/option/columns.name">http://datatables.net/reference/option/columns.name</a>
         */
        private String name;

        /**
         * Flag to indicate if this column is searchable (true) or not (false).
         *
         * @see <a href="http://datatables.net/reference/option/columns.searchable">http://datatables.net/reference/option/columns.searchable</a>
         */
        private boolean searchable;

        /**
         * Flag to indicate if this column is orderable (true) or not (false).
         *
         * @see <a href="http://datatables.net/reference/option/columns.orderable">http://datatables.net/reference/option/columns.orderable</a>
         */
        private boolean orderable;

        /**
         * Flag to indicate if column should be resolved (true) or not (false).
         */
        private boolean reference;

        /**
         * If 'reference' is true, this should be set to the exact collection name in the database which is used to resolve the reference.
         */
        private String referenceCollection;

        /**
         * If 'reference' is true, this should include a list of names of all columns that should be searched
         */
        private List<String> referenceColumns;

        /**
         * If 'reference' is true, this should be set to the name of the reference column which should be used to order the table.
         */
        private String referenceOrderColumn;

        /**
         * Sets how the column should be searched. E.g., if values should be parsed to integer and compared as integer. Set it to SearchType.Integer.
         */
        private SearchType searchType = SearchType.String;

        /**
         * Search value to apply to this specific column.
         */
        @NotNull
        private Search search;

        public enum SearchType {
            String,
            Boolean,
            Integer
        }
    }

    @Data
    public static final class Search {

        /**
         * Global search value. To be applied to all columns which have searchable as true.
         */
        @NotNull
        private String value;

        /**
         * true if the global filter should be treated as a regular expression for advanced searching,
         * false otherwise. Note that normally server-side processing scripts will not perform regular
         * expression searching for performance reasons on large data sets, but it is technically possible
         * and at the discretion of your script.
         */
        private boolean regex;

        public Search(@NotNull String value, boolean regex) {
            this.value = value;
            this.regex = regex;
        }

        Search() {}
    }

    @Data
    public static final class Order {

        /**
         * Column to which ordering should be applied. This is an index reference to the columns array of
         * information that is also submitted to the server.
         */
        @Min(0)
        private int column;

        /**
         * Ordering direction for this column. It will be asc or desc to indicate ascending ordering or
         * descending ordering, respectively.
         */
        @NotNull
        private Direction dir;

        public Order(@Min(0) int column, @NotNull Direction dir) {
            this.column = column;
            this.dir = dir;
        }

        Order() {}

        public enum Direction {
            desc, asc
        }
    }

}
