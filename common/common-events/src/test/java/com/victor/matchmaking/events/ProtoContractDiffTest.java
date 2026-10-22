package com.victor.matchmaking.events;

import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_OPTIONAL;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label.LABEL_REPEATED;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_INT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_MESSAGE;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_SINT32;
import static com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Type.TYPE_STRING;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.ENUM_VALUE_NUMBER_REUSED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.ENUM_VALUE_REMOVED_WITHOUT_RESERVED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.ENUM_VALUE_RENUMBERED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_KIND_CHANGED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_NAME_NOT_RESERVED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_NUMBER_REUSED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_PRESENCE_CHANGED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_REMOVED_WITHOUT_RESERVED;
import static com.victor.matchmaking.events.ProtoContractDiff.Code.FIELD_RENUMBERED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;
import com.victor.matchmaking.events.ProtoContractDiff.Code;
import com.victor.matchmaking.events.ProtoContractDiff.Report;

/**
 * The rules of ADR 0017, one test per rule.
 *
 * <p>The descriptors are built in code instead of being compiled from {@code .proto} text:
 * the rules are about what the wire sees, and a hand-built descriptor can express the
 * shape that protoc refuses to compile (a field number that is both reserved and in use)
 * or that only a future version of the tool could produce. The real contract is covered as
 * well - compared against itself and against a renumbering of itself - so these rules are
 * not only asserted on fixtures.
 */
class ProtoContractDiffTest {

    private static final String FILE = "matchmaking/events/sample.proto";
    private static final String GAME_MODE = "GameMode";
    private static final String REGION = "Region";

    @Test
    void identicalContractsAreCompatible() {
        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))),
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))));

        assertTrue(report.isCompatible(), report.lines()::toString);
        assertEquals(0, report.violations().size());
    }

    @Test
    void aNewFieldWithANewNumberIsCompatible() {
        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))),
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING), field("region", 4, TYPE_STRING))));

        assertTrue(report.isCompatible(), report.lines()::toString);
    }

    @Test
    void aNewFileIsCompatible() {
        FileDescriptorSet baseline = FileDescriptorSet.newBuilder()
                .addFile(file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))))
                .build();
        FileDescriptorSet current = baseline.toBuilder()
                .addFile(FileDescriptorProto.newBuilder()
                        .setName("matchmaking/events/match_cancelled.proto")
                        .addMessageType(message("MatchCancelled", field("match_id", 1, TYPE_STRING))))
                .build();

        assertTrue(ProtoContractDiff.compare(baseline, current).isCompatible());
    }

    @Test
    void renumberingAFieldIsRejected() {
        Report report = compare(
                file(message("MatchConfirmed", field("players", 5, TYPE_STRING))),
                file(message("MatchConfirmed", field("players", 6, TYPE_STRING))));

        assertViolation(FIELD_RENUMBERED, report);
    }

    @Test
    void renamingAFieldThatKeepsItsNumberIsRejected() {
        Report report = compare(
                file(message("MatchConfirmed", field("skill_at_match", 3, TYPE_INT32))),
                file(message("MatchConfirmed", field("skill_level", 3, TYPE_INT32))));

        // From the wire's side this is the same break as a renumbering: number 3 changed meaning.
        assertViolation(FIELD_NUMBER_REUSED, report);
    }

    @Test
    void removingAFieldWithoutReservingItsNumberIsRejected() {
        Report report = compare(
                file(message("MatchConfirmed", field("players", 5, TYPE_STRING))),
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))));

        assertViolation(FIELD_REMOVED_WITHOUT_RESERVED, report);
    }

    @Test
    void removingAFieldThatReservesNumberAndNameIsCompatible() {
        DescriptorProto after = message("MatchConfirmed", field("match_id", 2, TYPE_STRING)).toBuilder()
                .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(6))
                .addReservedName("players")
                .build();

        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING), field("players", 5, TYPE_STRING))),
                file(after));

        assertTrue(report.isCompatible(), report.lines()::toString);
    }

    @Test
    void reservingTheNumberOnlyIsAnAdvisory() {
        DescriptorProto after = message("MatchConfirmed", field("match_id", 2, TYPE_STRING)).toBuilder()
                .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(5).setEnd(6))
                .build();

        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING), field("players", 5, TYPE_STRING))),
                file(after));

        assertAdvisory(FIELD_NAME_NOT_RESERVED, report);
    }

    @Test
    void aReservedRangeCoversEveryNumberInIt() {
        // `reserved 3 to 5;` is stored as start=3, end=6 (end exclusive), so field 5 is covered.
        DescriptorProto after = message("MatchConfirmed", field("match_id", 2, TYPE_STRING)).toBuilder()
                .addReservedRange(DescriptorProto.ReservedRange.newBuilder().setStart(3).setEnd(6))
                .addReservedName("players")
                .build();

        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING), field("players", 5, TYPE_STRING))),
                file(after));

        assertTrue(report.isCompatible(), report.lines()::toString);
    }

    @Test
    void changingTheKindOfAFieldIsRejected() {
        Report report = compare(
                file(message("MatchPlayer", field("skill_at_match", 3, TYPE_INT32))),
                file(message("MatchPlayer", field("skill_at_match", 3, TYPE_SINT32))));

        assertViolation(FIELD_KIND_CHANGED, report);
    }

    @Test
    void changingTheLabelOfAFieldIsRejected() {
        Report report = compare(
                file(message("MatchConfirmed", field("players", 5, TYPE_INT32))),
                file(message("MatchConfirmed", field("players", 5, TYPE_INT32, LABEL_REPEATED))));

        assertViolation(FIELD_KIND_CHANGED, report);
    }

    @Test
    void changingTheMessageTypeOfAFieldIsRejected() {
        Report report = compare(
                file(message("MatchConfirmed", messageField("player", 5, ".matchmaking.events.MatchPlayerState"))),
                file(message("MatchConfirmed", messageField("player", 5, ".matchmaking.events.MatchPlayerRef"))));

        assertViolation(FIELD_KIND_CHANGED, report);
    }

    @Test
    void makingAFieldExplicitlyOptionalIsAnAdvisory() {
        Report report = compare(
                file(message("MatchPlayer", field("skill_at_match", 3, TYPE_INT32))),
                file(message("MatchPlayer", field("skill_at_match", 3, TYPE_INT32).toBuilder()
                        .setProto3Optional(true)
                        .build())));

        assertAdvisory(FIELD_PRESENCE_CHANGED, report);
    }

    @Test
    void fieldsOfANestedMessageAreComparedToo() {
        DescriptorProto baseline = message("MatchConfirmed", field("match_id", 2, TYPE_STRING)).toBuilder()
                .addNestedType(message("MatchPlayer", field("team", 2, TYPE_INT32)))
                .build();
        DescriptorProto current = message("MatchConfirmed", field("match_id", 2, TYPE_STRING)).toBuilder()
                .addNestedType(message("MatchPlayer", field("team", 3, TYPE_INT32)))
                .build();

        Report report = compare(file(baseline), file(current));

        assertViolation(FIELD_RENUMBERED, report);
        assertEquals("MatchConfirmed.MatchPlayer.team", report.violations().get(0).subject());
    }

    @Test
    void renumberingAnEnumValueIsRejected() {
        Report report = compare(
                file(enumType(GAME_MODE, value("GAME_MODE_1V1", 1))),
                file(enumType(GAME_MODE, value("GAME_MODE_1V1", 2))));

        assertViolation(ENUM_VALUE_RENUMBERED, report);
    }

    @Test
    void reusingTheNumberOfARemovedEnumValueIsRejected() {
        Report report = compare(
                file(enumType(GAME_MODE, value("GAME_MODE_1V1", 1), value("GAME_MODE_5V5", 3))),
                file(enumType(GAME_MODE, value("GAME_MODE_1V1", 1), value("GAME_MODE_2V2", 3))));

        assertViolation(ENUM_VALUE_NUMBER_REUSED, report);
    }

    @Test
    void removingAnEnumValueWithoutReservingItIsRejected() {
        Report report = compare(
                file(enumType(REGION, value("REGION_NA", 1), value("REGION_SA", 2))),
                file(enumType(REGION, value("REGION_NA", 1))));

        assertViolation(ENUM_VALUE_REMOVED_WITHOUT_RESERVED, report);
    }

    @Test
    void removingAnEnumValueThatReservesNumberAndNameIsCompatible() {
        // For an enum the reserved range is inclusive: start=2, end=2 covers value 2.
        EnumDescriptorProto after = enumType(REGION, value("REGION_NA", 1)).toBuilder()
                .addReservedRange(EnumDescriptorProto.EnumReservedRange.newBuilder().setStart(2).setEnd(2))
                .addReservedName("REGION_SA")
                .build();

        Report report = compare(
                file(enumType(REGION, value("REGION_NA", 1), value("REGION_SA", 2))),
                file(after));

        assertTrue(report.isCompatible(), report.lines()::toString);
    }

    @Test
    void theRealContractComparedWithItselfIsCompatible() {
        Report report = compare(realContract(), realContract());

        // The counters are asserted on purpose: "compatible" has to mean "compared and found
        // equal", not "compared nothing". The contract today is one file, two messages,
        // eight fields and the two enums it imports.
        assertTrue(report.isCompatible(), report.lines()::toString);
        assertTrue(report.fields() > 0, report.summary());
        assertTrue(report.messages() > 0, report.summary());
    }

    @Test
    void aRenumberingOfTheRealContractIsRejected() {
        Report report = compare(realContract(), renumberedPlayers(realContract()));

        assertViolation(FIELD_RENUMBERED, report);
        assertEquals("MatchConfirmed.players", report.violations().get(0).subject());
    }

    @Test
    void theSummaryLineStatesWhatWasCompared() {
        Report report = compare(
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))),
                file(message("MatchConfirmed", field("match_id", 2, TYPE_STRING))));

        // The shell scripts read this line; a rename here would silence the gate in CI.
        assertEquals(
                "contract.diff files=1 messages=1 fields=1 enums=0 enumValues=0 violations=0 advisories=0"
                        + " verdict=compatible",
                report.summary());
    }

    private static void assertViolation(Code code, Report report) {
        assertTrue(report.violations().stream().anyMatch(violation -> violation.code() == code),
                () -> code + " expected, got " + report.lines());
        assertFalse(report.isCompatible(), () -> code + " must fail the build");
    }

    private static void assertAdvisory(Code code, Report report) {
        assertTrue(report.violations().stream().anyMatch(violation -> violation.code() == code),
                () -> code + " expected, got " + report.lines());
        assertTrue(report.isCompatible(),
                () -> code + " is an advisory: it must be reported without failing the build");
    }

    private static Report compare(FileDescriptorProto baseline, FileDescriptorProto current) {
        return ProtoContractDiff.compare(set(baseline), set(current));
    }

    private static FileDescriptorSet set(FileDescriptorProto... files) {
        return FileDescriptorSet.newBuilder().addAllFile(List.of(files)).build();
    }

    private static FileDescriptorProto file(DescriptorProto... messages) {
        return FileDescriptorProto.newBuilder().setName(FILE).addAllMessageType(List.of(messages)).build();
    }

    private static FileDescriptorProto file(EnumDescriptorProto... enums) {
        return FileDescriptorProto.newBuilder().setName(FILE).addAllEnumType(List.of(enums)).build();
    }

    private static DescriptorProto message(String name, FieldDescriptorProto... fields) {
        return DescriptorProto.newBuilder().setName(name).addAllField(List.of(fields)).build();
    }

    private static FieldDescriptorProto field(String name, int number, FieldDescriptorProto.Type type) {
        return field(name, number, type, LABEL_OPTIONAL);
    }

    private static FieldDescriptorProto field(
            String name, int number, FieldDescriptorProto.Type type, FieldDescriptorProto.Label label) {
        return FieldDescriptorProto.newBuilder()
                .setName(name)
                .setNumber(number)
                .setType(type)
                .setLabel(label)
                .build();
    }

    private static FieldDescriptorProto messageField(String name, int number, String typeName) {
        return field(name, number, TYPE_MESSAGE).toBuilder().setTypeName(typeName).build();
    }

    private static EnumDescriptorProto enumType(String name, EnumValueDescriptorProto... values) {
        return EnumDescriptorProto.newBuilder().setName(name).addAllValue(List.of(values)).build();
    }

    private static EnumValueDescriptorProto value(String name, int number) {
        return EnumValueDescriptorProto.newBuilder().setName(name).setNumber(number).build();
    }

    /** The generated contract, as a consumer of it sees it: no protoc involved. */
    private static FileDescriptorProto realContract() {
        return MatchConfirmed.getDescriptor().getFile().toProto();
    }

    /** Moves `players` from 5 to 6 - the change the registry rates as compatible. */
    private static FileDescriptorProto renumberedPlayers(FileDescriptorProto file) {
        FileDescriptorProto.Builder builder = file.toBuilder();
        for (int typeIndex = 0; typeIndex < builder.getMessageTypeCount(); typeIndex++) {
            DescriptorProto.Builder message = builder.getMessageTypeBuilder(typeIndex);
            for (int index = 0; index < message.getFieldCount(); index++) {
                if (message.getField(index).getName().equals("players")) {
                    message.getFieldBuilder(index).setNumber(6);
                }
            }
        }
        return builder.build();
    }
}
