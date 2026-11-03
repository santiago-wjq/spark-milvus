package com.zilliz.milvus.jni.segcore;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * End-to-end probe of the segcore JNI in one JVM: a growing segment filled by
 * inserts, sealed segments loaded from V3 manifests, and retrieves with plans
 * built in the JVM.
 *
 * <p>It answers the build feasibility gate: the libraries of one Milvus build
 * load and run together in HotSpot. Plans are planpb.PlanNode messages built
 * here, as the connector will build them from Spark predicates, so no Go plan
 * parser is involved. The sealed segments are the api collection fixture's
 * (core/src/test/resources/read-fixtures/collections/api), loaded with only
 * their scalar fields, and every expected answer is computed from the
 * fixture's literal input rows, never from a read of the stored bytes. Run it
 * through run_probe.sh, which preloads the JRE's libjsig, copies the fixture
 * and lists its rows and manifest paths; it exits non-zero on the first
 * mismatch. Protobuf messages are encoded by hand so the probe needs no
 * generated classes; field numbers follow schema.proto, segcore.proto and
 * plan.proto at the pinned Milvus.
 */
public final class SegcoreProbe {
    private static final long PK = 100;
    private static final long AGE = 101;
    private static final long NAME = 102;
    private static final int INT64 = 5;
    private static final int VARCHAR = 21;

    // The api fixture: collection 10, partition 20, sealed segments 30 and 31.
    // Its column groups hold the vector alone, and the system fields with id
    // and category.
    private static final long API_COLLECTION = 10;
    private static final long API_PARTITION = 20;
    private static final long ID = 100;
    private static final long VECTOR = 101;
    private static final long CATEGORY = 102;
    private static final int FLOAT_VECTOR = 101;
    private static final int STORAGE_V3 = 3;

    // plan.proto OpType
    private static final int GREATER_THAN = 1;
    private static final int GREATER_EQUAL = 2;
    private static final int LESS_THAN = 3;
    private static final int EQUAL = 5;
    private static final int PREFIX_MATCH = 7;
    // BinaryExpr.BinaryOp and UnaryExpr.UnaryOp
    private static final int AND = 1;
    private static final int OR = 2;
    private static final int NOT = 1;

    private static final long[] PKS = {1, 2, 3, 4, 5, 6};
    private static final long[] AGES = {10, 25, 30, 45, 50, 65};
    private static final String[] NAMES = {"alice", "bob", "carol", "dave", "erin", "frank"};

    private SegcoreProbe() {}

    /** A row of the fixture's literal input: id and a category that is never null here. */
    private record Row(long id, long category) {}

    /**
     * @param args the local directory segcore owns, and the copied fixture's
     *     rows and manifest paths as tab-separated lines
     */
    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            throw new IllegalArgumentException("usage: SegcoreProbe <local dir> <fixture lines>");
        }
        System.loadLibrary("milvus-segcore-jni");
        SegcoreNative.init(Runtime.getRuntime().availableProcessors(), args[0], 1L << 30);
        growing();
        // The fixture takes a few KiB; 1 GiB of memory and disk is ample.
        SegcoreNative.configureCache(1L << 30, 1L << 30, 1L << 30, 1L << 30, 1L << 30, 1L << 30,
            Path.of(args[0], "cache").toString());
        sealed(Path.of(args[1]));
        System.out.println("segcore probe passed");
    }

    private static void growing() {
        long collection = SegcoreNative.newCollection(schema());
        long segment = SegcoreNative.newGrowingSegment(collection, 1);
        try {
            long[] timestamps = new long[PKS.length];
            Arrays.fill(timestamps, 100);
            SegcoreNative.insert(segment, PKS.clone(), timestamps, insertRecord());
            check("row count", SegcoreNative.rowCount(segment), PKS.length);

            expect(collection, segment, "age > 28 and age < 60",
                and(range(AGE, INT64, GREATER_THAN, int64(28)), range(AGE, INT64, LESS_THAN, int64(60))), 3, 4, 5);
            expect(collection, segment, "name starts with c", range(NAME, VARCHAR, PREFIX_MATCH, string("c")), 3);
            expect(collection, segment, "pk in [6, 2, 6]", term(PK, INT64, 6, 2, 6), 2, 6);
            expect(collection, segment, "age > 28 and not name == dave",
                and(range(AGE, INT64, GREATER_THAN, int64(28)), not(range(NAME, VARCHAR, EQUAL, string("dave")))),
                3, 5, 6);
            expect(collection, segment, "age < 15 or name == frank",
                or(range(AGE, INT64, LESS_THAN, int64(15)), range(NAME, VARCHAR, EQUAL, string("frank"))), 1, 6);
            expect(collection, segment, "age < 0", range(AGE, INT64, LESS_THAN, int64(0)));
            try {
                SegcoreNative.createRetrievePlan(collection, plan(range(999, INT64, EQUAL, int64(1)), PK, AGE));
                throw new AssertionError("a plan over an unknown field was accepted");
            } catch (SegcoreException expected) {
                System.out.println("rejected (code " + expected.code() + "): " + expected.getMessage());
            }
        } finally {
            SegcoreNative.deleteSegment(segment);
            SegcoreNative.deleteCollection(collection);
        }
    }

    /**
     * Sealed segments of the api fixture, loaded through its V3 manifests with
     * the collection's load fields set to id and category; the vector is never
     * read, since segcore loads fields outside that list only when a request
     * touches them. A cancelled load stops with an error and leaves the
     * segment loadable.
     */
    private static void sealed(Path lines) throws IOException {
        SegcoreNative.initLocalFileSystem();
        Map<Long, String> manifests = new HashMap<>();
        Map<Long, List<Row>> rows = new HashMap<>();
        readFixtureLines(lines, manifests, rows);

        long collection = SegcoreNative.newCollection(apiSchema());
        try {
            SegcoreNative.setLoadFields(collection, new long[] {ID, CATEGORY});

            List<Row> first = rows.get(30L);
            long segment = sealedSegment(collection, 30, first.size(), manifests);
            try {
                SegcoreNative.load(segment, 0);
                check("sealed row count", SegcoreNative.rowCount(segment), first.size());
                expectRows(collection, segment, "id >= 0", range(ID, INT64, GREATER_EQUAL, int64(0)), first,
                    r -> r.id() >= 0);
                expectRows(collection, segment, "category == 1", range(CATEGORY, INT64, EQUAL, int64(1)), first,
                    r -> r.category() == 1);
                expectRows(collection, segment, "id >= 3004 and category == 0",
                    and(range(ID, INT64, GREATER_EQUAL, int64(3004)), range(CATEGORY, INT64, EQUAL, int64(0))),
                    first, r -> r.id() >= 3004 && r.category() == 0);
                expectRows(collection, segment, "id in [3007, 3002, 3007, 9999]",
                    term(ID, INT64, 3007, 3002, 3007, 9999), first,
                    r -> r.id() == 3007 || r.id() == 3002 || r.id() == 9999);
                expectRows(collection, segment, "not category == 1 or id < 3002",
                    or(not(range(CATEGORY, INT64, EQUAL, int64(1))), range(ID, INT64, LESS_THAN, int64(3002))),
                    first, r -> r.category() != 1 || r.id() < 3002);
                expectRows(collection, segment, "id > 9000", range(ID, INT64, GREATER_THAN, int64(9000)), first,
                    r -> r.id() > 9000);
            } finally {
                SegcoreNative.deleteSegment(segment);
            }

            List<Row> second = rows.get(31L);
            long other = sealedSegment(collection, 31, second.size(), manifests);
            try {
                long cancellation = SegcoreNative.newLoadCancellation();
                try {
                    SegcoreNative.cancelLoad(cancellation);
                    try {
                        SegcoreNative.load(other, cancellation);
                        throw new AssertionError("a cancelled load completed");
                    } catch (SegcoreException expected) {
                        System.out.println("cancelled (code " + expected.code() + "): " + expected.getMessage());
                    }
                } finally {
                    SegcoreNative.releaseLoadCancellation(cancellation);
                }
                SegcoreNative.load(other, 0);
                expectRows(collection, other, "segment 31 after a cancelled load: category == 0",
                    range(CATEGORY, INT64, EQUAL, int64(0)), second, r -> r.category() == 0);
            } finally {
                SegcoreNative.deleteSegment(other);
            }
        } finally {
            SegcoreNative.deleteCollection(collection);
        }
    }

    /**
     * Lines from run_probe.sh: {@code manifest <segment> <manifest path>},
     * {@code row <segment> <id> <category>} and {@code delete <segment> <id>}.
     * Manifest base paths arrive absolute, as Milvus writes them for local
     * storage. The checks apply no deletes and cover no null categories, so a
     * fixture with either fails here rather than in a comparison.
     */
    private static void readFixtureLines(Path lines, Map<Long, String> manifests, Map<Long, List<Row>> rows)
        throws IOException {
        for (String line : Files.readAllLines(lines, StandardCharsets.UTF_8)) {
            String[] cols = line.split("\t", -1);
            long segment = Long.parseLong(cols[1]);
            switch (cols[0]) {
                case "manifest" -> manifests.put(segment, cols[2]);
                case "row" -> {
                    if (cols[3].equals("null")) {
                        throw new AssertionError("segment " + segment + " row " + cols[2] + " has a null category");
                    }
                    rows.computeIfAbsent(segment, s -> new ArrayList<>())
                        .add(new Row(Long.parseLong(cols[2]), Long.parseLong(cols[3])));
                }
                case "delete" -> throw new AssertionError("segment " + segment + " has deletes");
                default -> throw new IllegalArgumentException("unknown fixture line: " + line);
            }
        }
    }

    private static long sealedSegment(long collection, long segment, long rowCount, Map<Long, String> manifests) {
        String manifest = manifests.get(segment);
        if (manifest == null) {
            throw new AssertionError("no manifest path for segment " + segment);
        }
        return SegcoreNative.newSealedSegment(collection, segment, false, loadInfo(segment, rowCount, manifest));
    }

    private static void expect(long collection, long segment, String label, Proto predicate, long... want) {
        compare(label, retrieveSorted(collection, segment, plan(predicate, PK, AGE)), want);
    }

    private static void expectRows(long collection, long segment, String label, Proto predicate, List<Row> rows,
        Predicate<Row> matches) {
        long[] want = rows.stream().filter(matches).mapToLong(Row::id).sorted().toArray();
        compare(label, retrieveSorted(collection, segment, plan(predicate, ID, CATEGORY)), want);
    }

    private static long[] retrieveSorted(long collection, long segment, byte[] planNode) {
        long plan = SegcoreNative.createRetrievePlan(collection, planNode);
        try {
            long[] got = retrievedPks(SegcoreNative.retrieve(segment, plan, 1000, 1L << 30));
            Arrays.sort(got);
            return got;
        } finally {
            SegcoreNative.deleteRetrievePlan(plan);
        }
    }

    private static void compare(String label, long[] got, long[] want) {
        if (!Arrays.equals(got, want)) {
            throw new AssertionError(label + ": want " + Arrays.toString(want) + ", got " + Arrays.toString(got));
        }
        System.out.println("ok: " + label + " -> " + Arrays.toString(got));
    }

    // planpb.PlanNode{query=4 -> QueryPlanNode{predicates=1}, output_field_ids=3}
    private static byte[] plan(Proto predicate, long... outputs) {
        Proto query = new Proto().message(1, predicate);
        return new Proto().message(4, query).packedInt64(3, outputs).bytes();
    }

    // planpb.ColumnInfo{field_id=1, data_type=2, is_primary_key=3}
    private static Proto column(long field, int type) {
        return new Proto().int64(1, field).int64(2, type).bool(3, field == PK);
    }

    // planpb.Expr.unary_range_expr=5 -> UnaryRangeExpr{column_info=1, op=2, value=3}
    private static Proto range(long field, int type, int op, Proto value) {
        Proto expr = new Proto().message(1, column(field, type)).int64(2, op).message(3, value);
        return new Proto().message(5, expr);
    }

    // planpb.Expr.term_expr=1 -> TermExpr{column_info=1, values=2}. segcore
    // relies on sorted, distinct values (the plan parser's rewriter guarantees
    // it for online queries), so the list is normalized here.
    private static Proto term(long field, int type, long... values) {
        Proto expr = new Proto().message(1, column(field, type));
        for (long v : Arrays.stream(values).sorted().distinct().toArray()) {
            expr.message(2, int64(v));
        }
        return new Proto().message(1, expr);
    }

    // planpb.Expr.binary_expr=3 -> BinaryExpr{op=1, left=2, right=3}
    private static Proto and(Proto left, Proto right) {
        return binary(AND, left, right);
    }

    private static Proto or(Proto left, Proto right) {
        return binary(OR, left, right);
    }

    private static Proto binary(int op, Proto left, Proto right) {
        return new Proto().message(3, new Proto().int64(1, op).message(2, left).message(3, right));
    }

    // planpb.Expr.unary_expr=2 -> UnaryExpr{op=1, child=2}
    private static Proto not(Proto child) {
        return new Proto().message(2, new Proto().int64(1, NOT).message(2, child));
    }

    // planpb.GenericValue{int64_val=2, string_val=4}
    private static Proto int64(long v) {
        return new Proto().int64(2, v);
    }

    private static Proto string(String v) {
        return new Proto().string(4, v);
    }

    private static void check(String what, long got, long want) {
        if (got != want) {
            throw new AssertionError(what + ": want " + want + ", got " + got);
        }
    }

    // schema.CollectionSchema{name=1, fields=4}
    private static byte[] schema() {
        Proto pk = new Proto().int64(1, PK).string(2, "pk").bool(3, true).int64(5, INT64);
        Proto age = new Proto().int64(1, AGE).string(2, "age").int64(5, INT64);
        Proto maxLength = new Proto().string(1, "max_length").string(2, "64");
        Proto name = new Proto().int64(1, NAME).string(2, "name").int64(5, VARCHAR).message(6, maxLength);
        return new Proto().string(1, "probe").message(4, pk).message(4, age).message(4, name).bytes();
    }

    // The api fixture's schema as its snapshot.json records it, with the system
    // fields RootCoord appends to every collection (createCollectionTask.
    // appendSysFields): a sealed segment's column groups carry RowID and
    // Timestamp, and segcore resolves them through the schema, while snapshot
    // schemas list only the user's fields.
    // FieldSchema{description=4, type_params=6, nullable=15}; KeyValuePair{key=1, value=2}
    private static byte[] apiSchema() {
        Proto id = new Proto().int64(1, ID).string(2, "id").bool(3, true).int64(5, INT64);
        Proto dim = new Proto().string(1, "dim").string(2, "2");
        Proto vector = new Proto().int64(1, VECTOR).string(2, "vector").int64(5, FLOAT_VECTOR).message(6, dim);
        Proto category = new Proto().int64(1, CATEGORY).string(2, "category").int64(5, INT64).bool(15, true);
        Proto rowId = new Proto().int64(1, 0).string(2, "RowID").string(4, "row id").int64(5, INT64);
        Proto timestamp = new Proto().int64(1, 1).string(2, "Timestamp").string(4, "time stamp").int64(5, INT64);
        return new Proto().string(1, "local-collection").message(4, id).message(4, vector).message(4, category)
            .message(4, rowId).message(4, timestamp).bytes();
    }

    // segcore.SegmentLoadInfo{segmentID=1, partitionID=2, collectionID=3,
    // num_of_rows=7, storageVersion=15, manifest_path=21}
    private static byte[] loadInfo(long segment, long rowCount, String manifest) {
        return new Proto().int64(1, segment).int64(2, API_PARTITION).int64(3, API_COLLECTION).int64(7, rowCount)
            .int64(15, STORAGE_V3).string(21, manifest).bytes();
    }

    // segcore.InsertRecord{fields_data=1, num_rows=2}
    private static byte[] insertRecord() {
        return new Proto()
            .message(1, longField(PK, "pk", PKS))
            .message(1, longField(AGE, "age", AGES))
            .message(1, stringField(NAME, "name", NAMES))
            .int64(2, PKS.length)
            .bytes();
    }

    // schema.FieldData{type=1, field_name=2, scalars=3, field_id=5}; ScalarField.long_data=3
    private static Proto longField(long id, String name, long[] values) {
        Proto scalars = new Proto().message(3, new Proto().packedInt64(1, values));
        return new Proto().int64(1, INT64).string(2, name).message(3, scalars).int64(5, id);
    }

    // ScalarField.string_data=6, StringArray.data=1
    private static Proto stringField(long id, String name, String[] values) {
        Proto strings = new Proto();
        for (String v : values) {
            strings.string(1, v);
        }
        Proto scalars = new Proto().message(6, strings);
        return new Proto().int64(1, VARCHAR).string(2, name).message(3, scalars).int64(5, id);
    }

    // segcore.RetrieveResults.ids=1 -> schema.IDs.int_id=1 -> LongArray.data=1 (packed)
    private static long[] retrievedPks(byte[] results) {
        byte[] ids = Reader.field(results, 1);
        if (ids == null) {
            return new long[0];
        }
        byte[] intIds = Reader.field(ids, 1);
        if (intIds == null) {
            return new long[0];
        }
        // An empty LongArray omits its data field.
        byte[] data = Reader.field(intIds, 1);
        return data == null ? new long[0] : Reader.packedInt64(data);
    }

    /** Minimal protobuf writer for the probe's messages. */
    private static final class Proto {
        private final ByteArrayOutputStream out = new ByteArrayOutputStream();

        Proto int64(int field, long value) {
            tag(field, 0);
            varint(value);
            return this;
        }

        Proto bool(int field, boolean value) {
            return int64(field, value ? 1 : 0);
        }

        Proto string(int field, String value) {
            return bytes(field, value.getBytes(StandardCharsets.UTF_8));
        }

        Proto message(int field, Proto value) {
            return bytes(field, value.bytes());
        }

        Proto packedInt64(int field, long... values) {
            Proto packed = new Proto();
            for (long v : values) {
                packed.varint(v);
            }
            return bytes(field, packed.bytes());
        }

        byte[] bytes() {
            return out.toByteArray();
        }

        private Proto bytes(int field, byte[] value) {
            tag(field, 2);
            varint(value.length);
            out.write(value, 0, value.length);
            return this;
        }

        private void tag(int field, int wire) {
            varint(((long) field << 3) | wire);
        }

        private void varint(long v) {
            while ((v & ~0x7FL) != 0) {
                out.write((int) ((v & 0x7F) | 0x80));
                v >>>= 7;
            }
            out.write((int) v);
        }
    }

    /** Minimal protobuf reader: first length-delimited field and packed varints. */
    private static final class Reader {
        private final byte[] buf;
        private int pos;

        private Reader(byte[] buf) {
            this.buf = buf;
        }

        static byte[] field(byte[] message, int wanted) {
            Reader r = new Reader(message);
            while (r.pos < r.buf.length) {
                long key = r.varint();
                int field = (int) (key >>> 3);
                int wire = (int) (key & 7);
                if (wire == 2) {
                    int len = (int) r.varint();
                    if (field == wanted) {
                        return Arrays.copyOfRange(r.buf, r.pos, r.pos + len);
                    }
                    r.pos += len;
                } else if (wire == 0) {
                    r.varint();
                } else if (wire == 1) {
                    r.pos += 8;
                } else if (wire == 5) {
                    r.pos += 4;
                } else {
                    throw new IllegalStateException("unsupported wire type " + wire);
                }
            }
            return null;
        }

        static long[] packedInt64(byte[] packed) {
            Reader r = new Reader(packed);
            List<Long> values = new ArrayList<>();
            while (r.pos < r.buf.length) {
                values.add(r.varint());
            }
            return values.stream().mapToLong(Long::longValue).toArray();
        }

        private long varint() {
            long result = 0;
            for (int shift = 0; ; shift += 7) {
                byte b = buf[pos++];
                result |= (long) (b & 0x7F) << shift;
                if ((b & 0x80) == 0) {
                    return result;
                }
            }
        }
    }
}
