package com.victor.matchmaking.events;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.function.Function;
import java.util.stream.Collectors;

import com.google.protobuf.DescriptorProtos.DescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumDescriptorProto;
import com.google.protobuf.DescriptorProtos.EnumValueDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto;
import com.google.protobuf.DescriptorProtos.FieldDescriptorProto.Label;
import com.google.protobuf.DescriptorProtos.FileDescriptorProto;
import com.google.protobuf.DescriptorProtos.FileDescriptorSet;

/**
 * The evolution rules of an event contract that the Schema Registry's compatibility
 * level cannot check (ADR 0017).
 *
 * <p>The registry rates a Protobuf change with the field <em>name</em> as the identity of
 * a field, and that is why {@code FULL} accepts a renumbering, a removal and a rename
 * (measured in ADR 0015). Those are exactly the changes that break a consumer which is
 * still running the previous code: the reader of a renumbered field silently reads
 * whatever else now carries that number, and a removal takes the field away from a
 * producer that has not been deployed yet. This tool compares the two contracts as the
 * <em>wire</em> sees them - numbers, kinds and enum values - and fails the build on the
 * differences the registry considers compatible.
 *
 * <p>Input: two {@code FileDescriptorSet}s, the baseline and the current contract, both
 * produced by {@code protoc --include_imports} (see
 * {@code requests/kafka/check_proto_evolution.sh}, which is what CI runs). Comparing
 * descriptors and not the {@code .proto} text keeps the check blind to comments,
 * formatting and field order, which the wire does not see either.
 *
 * <p>Exit code: 0 when the change is compatible, 1 when a rule was broken, 2 when it was
 * called wrong. Advisories are printed and do not fail the build: they describe a change
 * that is legal on the wire but that a human still has to look at.
 */
public final class ProtoContractDiff {

    /** What a difference between the two contracts means for the peers already deployed. */
    public enum Severity {
        /** A consumer or a producer of the previous contract breaks. Fails the build. */
        VIOLATION,
        /** Legal on the wire, worth a second look. Printed, does not fail the build. */
        ADVISORY,
    }

    /** The rules. The name of the constant is the name printed by the tool and by CI. */
    public enum Code {
        /** A field moved to another number: a reader of the old contract reads the wrong field. */
        FIELD_RENUMBERED(Severity.VIOLATION),
        /** A new field took the number of a field that is gone: the same wire break from the other side. */
        FIELD_NUMBER_REUSED(Severity.VIOLATION),
        /** A field is gone and its number is not {@code reserved}: the number is free to be reused by mistake. */
        FIELD_REMOVED_WITHOUT_RESERVED(Severity.VIOLATION),
        /** The number kept its meaning but changed type or label: the value is decoded as something else. */
        FIELD_KIND_CHANGED(Severity.VIOLATION),
        /** The number is {@code reserved} but the name is not: no wire break, a trap for the next reader. */
        FIELD_NAME_NOT_RESERVED(Severity.ADVISORY),
        /** Presence became explicit: a writer may now publish the default value where it used to omit it. */
        FIELD_PRESENCE_CHANGED(Severity.ADVISORY),
        /** An enum value was renumbered: the old writer's number now means another value. */
        ENUM_VALUE_RENUMBERED(Severity.VIOLATION),
        /** An enum value number was reused by a value with another name: the same break from the other side. */
        ENUM_VALUE_NUMBER_REUSED(Severity.VIOLATION),
        /** An enum value is gone and its number is not {@code reserved}. */
        ENUM_VALUE_REMOVED_WITHOUT_RESERVED(Severity.VIOLATION);

        private final Severity severity;

        Code(Severity severity) {
            this.severity = severity;
        }

        public Severity severity() {
            return severity;
        }
    }

    /** One difference, as the tool prints it. */
    public record Violation(Code code, String subject, String detail) {

        public String line() {
            return "%s %s: %s".formatted(code, subject, detail);
        }
    }

    /**
     * The result of a comparison, with the size of what was compared: the counters are what
     * makes "clean" a statement about a contract that was really read instead of a run that
     * compared nothing.
     */
    public record Report(
            List<Violation> violations, int files, int messages, int fields, int enums, int enumValues) {

        public boolean isCompatible() {
            return violations.stream().noneMatch(violation -> violation.code().severity() == Severity.VIOLATION);
        }

        public List<String> lines() {
            return violations.stream().map(Violation::line).toList();
        }

        public String summary() {
            String verdict = isCompatible() ? "compatible" : "breaking";
            Map<Severity, Long> bySeverity = violations.stream()
                    .collect(Collectors.groupingBy(violation -> violation.code().severity(), Collectors.counting()));
            return "contract.diff files=%d messages=%d fields=%d enums=%d enumValues=%d violations=%d advisories=%d verdict=%s"
                    .formatted(
                            files,
                            messages,
                            fields,
                            enums,
                            enumValues,
                            bySeverity.getOrDefault(Severity.VIOLATION, 0L),
                            bySeverity.getOrDefault(Severity.ADVISORY, 0L),
                            verdict);
        }
    }

    private ProtoContractDiff() {
    }

    public static void main(String[] args) throws IOException {
        if (args.length != 2) {
            System.err.println("usage: ProtoContractDiff <baseline.desc> <current.desc>"
                    + "  (both from: protoc --include_imports --descriptor_set_out=...)");
            System.exit(2);
        }
        Report report = compare(parse(Path.of(args[0])), parse(Path.of(args[1])));
        report.lines().forEach(System.out::println);
        System.out.println(report.summary());
        if (!report.isCompatible()) {
            System.exit(1);
        }
    }

    private static FileDescriptorSet parse(Path path) throws IOException {
        return FileDescriptorSet.parseFrom(Files.readAllBytes(path));
    }

    /** Compares two contracts. Both carry their imports ({@code --include_imports}). */
    public static Report compare(FileDescriptorSet baseline, FileDescriptorSet current) {
        Comparison comparison = new Comparison();
        Map<String, FileDescriptorProto> currentFiles = indexByName(current.getFileList(), FileDescriptorProto::getName);
        for (FileDescriptorProto baselineFile : baseline.getFileList()) {
            FileDescriptorProto currentFile = currentFiles.get(baselineFile.getName());
            if (currentFile == null) {
                // The file left the contract completely: nothing a peer could still import
                // is around to break. Re-adding it later re-adds a new contract.
                continue;
            }
            comparison.files++;
            comparison.scope("", baselineFile, currentFile);
        }
        return comparison.report();
    }

    /** Walks both contracts in parallel and collects the differences. */
    private static final class Comparison {

        private final List<Violation> violations = new ArrayList<>();
        private int files;
        private int messages;
        private int fields;
        private int enums;
        private int enumValues;

        void scope(String prefix, FileDescriptorProto baseline, FileDescriptorProto current) {
            messages(prefix, baseline.getMessageTypeList(), current.getMessageTypeList());
            enums(prefix, baseline.getEnumTypeList(), current.getEnumTypeList());
        }

        private void messages(String prefix, List<DescriptorProto> baseline, List<DescriptorProto> current) {
            Map<String, DescriptorProto> currentByName = indexByName(current, DescriptorProto::getName);
            for (DescriptorProto baselineMessage : baseline) {
                DescriptorProto currentMessage = currentByName.get(baselineMessage.getName());
                if (currentMessage == null) {
                    // A message that is gone is a compile-time break for the producer and a
                    // contract of its own: its numbers cannot clash with anything that stays.
                    continue;
                }
                messages++;
                String qualifiedName = prefix + baselineMessage.getName();
                fields(qualifiedName, baselineMessage, currentMessage);
                enums(qualifiedName + ".", baselineMessage.getEnumTypeList(), currentMessage.getEnumTypeList());
                messages(qualifiedName + ".", baselineMessage.getNestedTypeList(), currentMessage.getNestedTypeList());
            }
        }

        private void fields(String messageName, DescriptorProto baseline, DescriptorProto current) {
            Map<String, FieldDescriptorProto> currentByName =
                    indexByName(current.getFieldList(), FieldDescriptorProto::getName);
            Map<Integer, FieldDescriptorProto> currentByNumber = indexByNumber(current.getFieldList(), FieldDescriptorProto::getNumber);
            Set<Integer> reservedNumbers = reservedNumbers(current);
            Set<String> reservedNames = new TreeSet<>(current.getReservedNameList());

            for (FieldDescriptorProto baselineField : baseline.getFieldList()) {
                fields++;
                String subject = messageName + "." + baselineField.getName();
                int number = baselineField.getNumber();
                FieldDescriptorProto currentField = currentByName.get(baselineField.getName());
                if (currentField != null) {
                    if (currentField.getNumber() != number) {
                        violations.add(new Violation(Code.FIELD_RENUMBERED, subject,
                                "field number " + number + " became " + currentField.getNumber()
                                        + "; the wire is read by number, so a reader of the old contract"
                                        + " decodes whatever else took that number. Add a new field and stop"
                                        + " writing the old one instead."));
                    } else if (!kind(baselineField).equals(kind(currentField))) {
                        violations.add(new Violation(Code.FIELD_KIND_CHANGED, subject,
                                "was %s, is now %s".formatted(kind(baselineField), kind(currentField))));
                    } else if (baselineField.hasProto3Optional() != currentField.hasProto3Optional()) {
                        violations.add(new Violation(Code.FIELD_PRESENCE_CHANGED, subject,
                                "presence changed from %s to %s; a reader can now be handed the default value"
                                        .formatted(
                                                baselineField.hasProto3Optional() ? "explicit" : "implicit",
                                                currentField.hasProto3Optional() ? "explicit" : "implicit")));
                    }
                    continue;
                }

                FieldDescriptorProto reusedBy = currentByNumber.get(number);
                if (reusedBy != null) {
                    violations.add(new Violation(Code.FIELD_NUMBER_REUSED, subject,
                            "number " + number + " is now " + reusedBy.getName()
                                    + "; a writer of the old contract has that number carrying a value of"
                                    + " another type. Reserve the number instead."));
                } else if (!reservedNumbers.contains(number)) {
                    violations.add(new Violation(Code.FIELD_REMOVED_WITHOUT_RESERVED, subject,
                            "field number " + number + " is gone and is not in `reserved`, so the number is"
                                    + " free to be reused by accident. Remove a field with `reserved " + number
                                    + ";` and `reserved \"" + baselineField.getName() + "\";`."));
                } else if (!reservedNames.contains(baselineField.getName())) {
                    violations.add(new Violation(Code.FIELD_NAME_NOT_RESERVED, subject,
                            "number " + number + " is reserved but the name is not; reserve the name as well"
                                    + " to keep a future field from being read as this one."));
                }
            }
        }

        private void enums(String prefix, List<EnumDescriptorProto> baseline, List<EnumDescriptorProto> current) {
            Map<String, EnumDescriptorProto> currentByName = indexByName(current, EnumDescriptorProto::getName);
            for (EnumDescriptorProto baselineEnum : baseline) {
                EnumDescriptorProto currentEnum = currentByName.get(baselineEnum.getName());
                if (currentEnum == null) {
                    continue;
                }
                enums++;
                enumValues(prefix + baselineEnum.getName(), baselineEnum, currentEnum);
            }
        }

        private void enumValues(String enumName, EnumDescriptorProto baseline, EnumDescriptorProto current) {
            Map<String, EnumValueDescriptorProto> currentByName =
                    indexByName(current.getValueList(), EnumValueDescriptorProto::getName);
            Map<Integer, EnumValueDescriptorProto> currentByNumber = indexByNumber(current.getValueList(), EnumValueDescriptorProto::getNumber);
            Set<Integer> reservedNumbers = reservedNumbers(current);
            Set<String> reservedNames = new TreeSet<>(current.getReservedNameList());

            for (EnumValueDescriptorProto baselineValue : baseline.getValueList()) {
                enumValues++;
                String subject = enumName + "." + baselineValue.getName();
                int number = baselineValue.getNumber();
                EnumValueDescriptorProto currentValue = currentByName.get(baselineValue.getName());
                if (currentValue != null) {
                    if (currentValue.getNumber() != number) {
                        violations.add(new Violation(Code.ENUM_VALUE_RENUMBERED, subject,
                                "value number " + number + " became " + currentValue.getNumber()
                                        + "; a writer of the old contract publishes " + number
                                        + " meaning another value now"));
                    }
                    continue;
                }

                EnumValueDescriptorProto reusedBy = currentByNumber.get(number);
                if (reusedBy != null) {
                    violations.add(new Violation(Code.ENUM_VALUE_NUMBER_REUSED, subject,
                            "value number " + number + " is now " + reusedBy.getName()
                                    + "; a writer of the old contract publishes an enum value that reads as"
                                    + " another value"));
                } else if (!reservedNumbers.contains(number)) {
                    violations.add(new Violation(Code.ENUM_VALUE_REMOVED_WITHOUT_RESERVED, subject,
                            "value number " + number + " is gone and is not in `reserved`, so a writer of the"
                                    + " old contract can send a value that no longer resolves. Remove an enum"
                                    + " value with `reserved " + number + ";` and `reserved \""
                                    + baselineValue.getName() + "\";`."));
                } else if (!reservedNames.contains(baselineValue.getName())) {
                    violations.add(new Violation(Code.FIELD_NAME_NOT_RESERVED, subject,
                            "number " + number + " is reserved but the name is not; reserve the name as well"));
                }
            }
        }

        Report report() {
            return new Report(
                    List.copyOf(violations), files, messages, fields, enums, enumValues);
        }
    }

    /**
     * The type and label of a field as the wire sees them. A message or enum field is
     * identified by the name of its type as well: {@code TYPE_MESSAGE} alone would rate a
     * change from one message type to another as no change at all.
     */
    private static String kind(FieldDescriptorProto field) {
        String label = field.getLabel() == Label.LABEL_REPEATED ? "repeated " : "";
        String type = field.getType().name().replace("TYPE_", "").toLowerCase(Locale.ROOT);
        if (field.hasTypeName()) {
            type = type + "<" + field.getTypeName() + ">";
        }
        return label + type;
    }

    /** The numbers a message reserved. `end` is exclusive: `reserved 4 to 6;` is stored as 4..7. */
    private static Set<Integer> reservedNumbers(DescriptorProto message) {
        Set<Integer> numbers = new TreeSet<>();
        for (DescriptorProto.ReservedRange range : message.getReservedRangeList()) {
            for (int number = range.getStart(); number < range.getEnd(); number++) {
                numbers.add(number);
            }
        }
        return numbers;
    }

    /** The numbers an enum reserved. `end` is INCLUSIVE here: `reserved 4 to 6;` is stored as 4..6. */
    private static Set<Integer> reservedNumbers(EnumDescriptorProto enumType) {
        Set<Integer> numbers = new TreeSet<>();
        for (EnumDescriptorProto.EnumReservedRange range : enumType.getReservedRangeList()) {
            for (int number = range.getStart(); number <= range.getEnd(); number++) {
                numbers.add(number);
            }
        }
        return numbers;
    }

    private static <T> Map<Integer, T> indexByNumber(List<T> descriptors, Function<T, Integer> number) {
        Map<Integer, T> index = new LinkedHashMap<>();
        for (T descriptor : descriptors) {
            index.putIfAbsent(number.apply(descriptor), descriptor);
        }
        return index;
    }

    private static <T> Map<String, T> indexByName(List<T> descriptors, Function<T, String> name) {
        Map<String, T> index = new LinkedHashMap<>();
        for (T descriptor : descriptors) {
            index.putIfAbsent(name.apply(descriptor), descriptor);
        }
        return index;
    }
}
