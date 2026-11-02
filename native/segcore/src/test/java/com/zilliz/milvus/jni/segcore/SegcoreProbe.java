package com.zilliz.milvus.jni.segcore;

import java.io.ByteArrayOutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * End-to-end probe of the segcore JNI in one JVM: collection, growing segment
 * insert, and retrieves with plans built in the JVM.
 *
 * <p>It answers the build feasibility gate: the libraries of one Milvus build
 * load and run together in HotSpot. Plans are planpb.PlanNode messages built
 * here, as the connector will build them from Spark predicates, so no Go plan
 * parser is involved. Run it with the JRE's libjsig preloaded and
 * java.library.path naming the Milvus output lib directory; it exits non-zero
 * on the first mismatch. Protobuf messages are encoded by hand so the probe
 * needs no generated classes; field numbers follow schema.proto,
 * segcore.proto and plan.proto at the pinned Milvus.
 */
public final class SegcoreProbe {
    private static final long PK = 100;
    private static final long AGE = 101;
    private static final long NAME = 102;
    private static final int INT64 = 5;
    private static final int VARCHAR = 21;

    // plan.proto OpType
    private static final int GREATER_THAN = 1;
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

    public static void main(String[] args) {
        System.loadLibrary("milvus-segcore-jni");
        String localDir = args.length > 0 ? args[0] : System.getProperty("java.io.tmpdir") + "/segcore-probe";
        SegcoreNative.init(Runtime.getRuntime().availableProcessors(), localDir, 1L << 30);

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
                SegcoreNative.createRetrievePlan(collection, plan(range(999, INT64, EQUAL, int64(1))));
                throw new AssertionError("a plan over an unknown field was accepted");
            } catch (SegcoreException expected) {
                System.out.println("rejected (code " + expected.code() + "): " + expected.getMessage());
            }
        } finally {
            SegcoreNative.deleteSegment(segment);
            SegcoreNative.deleteCollection(collection);
        }
        System.out.println("segcore probe passed");
    }

    private static void expect(long collection, long segment, String label, Proto predicate, long... want) {
        long plan = SegcoreNative.createRetrievePlan(collection, plan(predicate));
        try {
            byte[] results = SegcoreNative.retrieve(segment, plan, 1000, 1L << 30);
            long[] got = retrievedPks(results);
            Arrays.sort(got);
            if (!Arrays.equals(got, want)) {
                throw new AssertionError(label + ": want " + Arrays.toString(want) + ", got " + Arrays.toString(got));
            }
            System.out.println("ok: " + label + " -> " + Arrays.toString(got));
        } finally {
            SegcoreNative.deleteRetrievePlan(plan);
        }
    }

    // planpb.PlanNode{query=4 -> QueryPlanNode{predicates=1}, output_field_ids=3}
    private static byte[] plan(Proto predicate) {
        Proto query = new Proto().message(1, predicate);
        return new Proto().message(4, query).packedInt64(3, PK, AGE).bytes();
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
