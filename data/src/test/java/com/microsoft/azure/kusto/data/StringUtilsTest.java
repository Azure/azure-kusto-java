// Copyright (c) Microsoft Corporation.
// Licensed under the MIT License.

package com.microsoft.azure.kusto.data;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test class for the string utility methods added to Utils.java.
 * These methods replace functionality previously provided by Apache Commons Lang3 StringUtils.
 */
class StringUtilsTest {

    // ========== Tests for isBlank() ==========

    @Test
    @DisplayName("isBlank should return true for null string")
    void isBlank_NullString_ReturnsTrue() {
        assertTrue(StringUtils.isBlank(null));
    }

    @Test
    @DisplayName("isBlank should return true for empty string")
    void isBlank_EmptyString_ReturnsTrue() {
        assertTrue(StringUtils.isBlank(""));
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "  ", "\t", "\n", "\r", " \t\n\r "})
    @DisplayName("isBlank should return true for whitespace-only strings")
    void isBlank_WhitespaceOnlyStrings_ReturnsTrue(String input) {
        assertTrue(StringUtils.isBlank(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", " a ", "hello", "  hello  ", "\tworld\n", "123", "!@#"})
    @DisplayName("isBlank should return false for strings with non-whitespace content")
    void isBlank_NonWhitespaceStrings_ReturnsFalse(String input) {
        assertFalse(StringUtils.isBlank(input));
    }

    // ========== Tests for isNotBlank() ==========

    @Test
    @DisplayName("isNotBlank should return false for null string")
    void isNotBlank_NullString_ReturnsFalse() {
        assertFalse(StringUtils.isNotBlank(null));
    }

    @Test
    @DisplayName("isNotBlank should return false for empty string")
    void isNotBlank_EmptyString_ReturnsFalse() {
        assertFalse(StringUtils.isNotBlank(""));
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "  ", "\t", "\n", "\r", " \t\n\r "})
    @DisplayName("isNotBlank should return false for whitespace-only strings")
    void isNotBlank_WhitespaceOnlyStrings_ReturnsFalse(String input) {
        assertFalse(StringUtils.isNotBlank(input));
    }

    @ParameterizedTest
    @ValueSource(strings = {"a", " a ", "hello", "  hello  ", "\tworld\n", "123", "!@#"})
    @DisplayName("isNotBlank should return true for strings with non-whitespace content")
    void isNotBlank_NonWhitespaceStrings_ReturnsTrue(String input) {
        assertTrue(StringUtils.isNotBlank(input));
    }

    @Test
    @DisplayName("isNotBlank should match Apache Commons Lang3 specification examples")
    void isNotBlank_ApacheCommonsSpecificationExamples() {
        // Direct examples from Apache Commons Lang3 documentation:
        // StringUtils.isNotBlank(null)      = false
        // StringUtils.isNotBlank("")        = false
        // StringUtils.isNotBlank(" ")       = false
        // StringUtils.isNotBlank("bob")     = true
        // StringUtils.isNotBlank("  bob  ") = true
        
        assertFalse(StringUtils.isNotBlank(null), "isNotBlank(null) should return false");
        assertFalse(StringUtils.isNotBlank(""), "isNotBlank(\"\") should return false");
        assertFalse(StringUtils.isNotBlank(" "), "isNotBlank(\" \") should return false");
        assertTrue(StringUtils.isNotBlank("bob"), "isNotBlank(\"bob\") should return true");
        assertTrue(StringUtils.isNotBlank("  bob  "), "isNotBlank(\"  bob  \") should return true");
    }

    @Test
    @DisplayName("isNotBlank should work with different CharSequence types")
    void isNotBlank_DifferentCharSequenceTypes() {
        // Test with String
        assertTrue(StringUtils.isNotBlank("hello"));
        assertFalse(StringUtils.isNotBlank("   "));
        
        // Test with StringBuilder
        StringBuilder sb = new StringBuilder("world");
        assertTrue(StringUtils.isNotBlank(sb));
        
        StringBuilder emptySb = new StringBuilder("   ");
        assertFalse(StringUtils.isNotBlank(emptySb));
        
        // Test with StringBuffer
        StringBuffer sbuf = new StringBuffer("test");
        assertTrue(StringUtils.isNotBlank(sbuf));
        
        StringBuffer emptyBuf = new StringBuffer("");
        assertFalse(StringUtils.isNotBlank(emptyBuf));
    }

    // ========== Tests for isEmpty() ==========

    @Test
    @DisplayName("isEmpty should return true for null string")
    void isEmpty_NullString_ReturnsTrue() {
        assertTrue(StringUtils.isEmpty(null));
    }

    @Test
    @DisplayName("isEmpty should return true for empty string")
    void isEmpty_EmptyString_ReturnsTrue() {
        assertTrue(StringUtils.isEmpty(""));
    }

    @ParameterizedTest
    @ValueSource(strings = {" ", "a", "hello", "\t", "\n"})
    @DisplayName("isEmpty should return false for non-empty strings")
    void isEmpty_NonEmptyStrings_ReturnsFalse(String input) {
        assertFalse(StringUtils.isEmpty(input));
    }

    // ========== Tests for chop() (basic character removal) ==========

    @Test
    @DisplayName("chop should return null for null string")
    void chop_NullString_ReturnsNull() {
        assertNull(StringUtils.chop(null));
    }

    @ParameterizedTest
    @MethodSource("chopBasicTestCases")
    @DisplayName("chop should handle basic cases correctly")
    void chop_BasicCases_WorksCorrectly(String input, String expected) {
        assertEquals(expected, StringUtils.chop(input));
    }

    @ParameterizedTest
    @MethodSource("chopSingleCharTestCases")
    @DisplayName("chop should return empty string for single character strings")
    void chop_SingleCharStrings_ReturnsEmpty(String input) {
        assertEquals("", StringUtils.chop(input));
    }

    @ParameterizedTest
    @MethodSource("chopSpecialCharTestCases")
    @DisplayName("chop should handle special characters correctly")
    void chop_SpecialChars_RemovesLastChar(String input, String expected) {
        assertEquals(expected, StringUtils.chop(input));
    }

    @ParameterizedTest
    @MethodSource("chopUnicodeTestCases")
    @DisplayName("chop should handle unicode characters correctly")
    void chop_UnicodeChars_RemovesLastChar(String input, String expected) {
        assertEquals(expected, StringUtils.chop(input));
    }

    // ========== Tests for chopNewLine() (line ending aware removal) ==========

    @Test
    @DisplayName("chopNewLine should return null for null string")
    void chopNewLine_NullString_ReturnsNull() {
        assertNull(StringUtils.chopNewLine(null));
    }

    @ParameterizedTest
    @MethodSource("chopCRLFTestCases")
    @DisplayName("chopNewLine should handle CRLF line endings specially")
    void chopNewLine_CRLFLineEndings_RemovesBothChars(String input, String expected) {
        assertEquals(expected, StringUtils.chopNewLine(input));
    }

    @ParameterizedTest
    @MethodSource("chopSingleLFTestCases")
    @DisplayName("chopNewLine should handle single LF normally")
    void chopNewLine_SingleLF_RemovesOnlyLF(String input, String expected) {
        assertEquals(expected, StringUtils.chopNewLine(input));
    }

    @ParameterizedTest
    @MethodSource("chopSingleCRTestCases")
    @DisplayName("chopNewLine should handle single CR normally")
    void chopNewLine_SingleCR_RemovesOnlyCR(String input, String expected) {
        assertEquals(expected, StringUtils.chopNewLine(input));
    }

    @ParameterizedTest
    @MethodSource("chopLFCRTestCases")
    @DisplayName("chopNewLine should handle LF followed by CR normally (not CRLF)")
    void chopNewLine_LFCRNotCRLF_RemovesOnlyLastChar(String input, String expected) {
        assertEquals(expected, StringUtils.chopNewLine(input));
    }

    @ParameterizedTest
    @MethodSource("chopMultipleCRLFTestCases")
    @DisplayName("chopNewLine should handle multiple CRLF patterns")
    void chopNewLine_MultipleCRLF_OnlyAffectsLast(String input, String expected) {
        assertEquals(expected, StringUtils.chopNewLine(input));
    }

    static Stream<Arguments> chopBasicTestCases() {
        return Stream.of(
            Arguments.of("", ""), // Empty string returns empty
            Arguments.of("ab", "a"),
            Arguments.of("hello", "hell"),
            Arguments.of("world!", "world"),
            Arguments.of("  spaces  ", "  spaces "),
            Arguments.of("tab\t", "tab"),
            Arguments.of("123456", "12345"),
            Arguments.of("abc", "ab"),
            Arguments.of("test string", "test strin"),
            Arguments.of("This is a longer test string", "This is a longer test strin"),
            // Basic line ending handling (chop just removes last char)
            Arguments.of("hello\n", "hello"),
            Arguments.of("test\r", "test"),
            Arguments.of("hello\r\n", "hello\r") // chop removes only the last char
        );
    }

    static Stream<String> chopSingleCharTestCases() {
        return Stream.of("a", " ", "\n", "\r", "x", "€", "1", "!");
    }

    static Stream<Arguments> chopCRLFTestCases() {
        return Stream.of(
            Arguments.of("\r\n", ""), // CRLF only
            Arguments.of("hello\r\n", "hello"),
            Arguments.of("test\r\n", "test"),
            Arguments.of("multiline\r\n", "multiline"),
            Arguments.of("a\r\n", "") // Special case: single char + CRLF
        );
    }

    static Stream<Arguments> chopSingleLFTestCases() {
        return Stream.of(
            Arguments.of("hello\n", "hello"),
            Arguments.of("test\n", "test"),
            Arguments.of("a\n", ""), // Two-char string with LF
            Arguments.of("text\n", "text")
        );
    }

    static Stream<Arguments> chopSingleCRTestCases() {
        return Stream.of(
            Arguments.of("hello\r", "hello"),
            Arguments.of("test\r", "test"),
            Arguments.of("a\r", ""), // Two-char string with CR
            Arguments.of("text\r", "text")
        );
    }

    static Stream<Arguments> chopLFCRTestCases() {
        return Stream.of(
            // \n\r is not the same as \r\n, should only remove the last \r
            Arguments.of("hello\n\r", "hello\n"),
            Arguments.of("test\n\r", "test\n"),
            Arguments.of("text\n\r", "text\n")
        );
    }

    static Stream<Arguments> chopMultipleCRLFTestCases() {
        return Stream.of(
            Arguments.of("line1\r\nline2\r\n", "line1\r\nline2"),
            Arguments.of("a\r\nb\r\n", "a\r\nb"),
            Arguments.of("\r\ntest\r\n", "\r\ntest")
        );
    }

    static Stream<Arguments> chopSpecialCharTestCases() {
        return Stream.of(
            Arguments.of("quote\"", "quote"),
            Arguments.of("slash\\", "slash"),
            Arguments.of("percent%", "percent"),
            Arguments.of("hash#", "hash"),
            Arguments.of("dollar$", "dollar"),
            Arguments.of("ampersand&", "ampersand"),
            Arguments.of("asterisk*", "asterisk"),
            Arguments.of("parenthesis(", "parenthesis"),
            Arguments.of("bracket[", "bracket"),
            Arguments.of("brace{", "brace")
        );
    }

    static Stream<Arguments> chopUnicodeTestCases() {
        return Stream.of(
            Arguments.of("unicode€", "unicode"),
            Arguments.of("café", "caf"),
            Arguments.of("naïve", "naïv"),
            Arguments.of("resumé", "resum"),
            Arguments.of("señor", "seño"),
            Arguments.of("北京", "北"),
            Arguments.of("🌍world", "🌍worl"),
            Arguments.of("te🚀st", "te🚀s")
        );
    }

    // ========== Tests for removeEnd() ==========

    @Test
    @DisplayName("removeEnd should return null for null string")
    void removeEnd_NullString_ReturnsNull() {
        assertNull(StringUtils.removeEnd(null, "suffix"));
    }

    @Test
    @DisplayName("removeEnd should return original string when suffix is null")
    void removeEnd_NullSuffix_ReturnsOriginal() {
        assertEquals("hello", StringUtils.removeEnd("hello", null));
    }

    @ParameterizedTest
    @MethodSource("removeEndTestCases")
    @DisplayName("removeEnd should remove suffix when present (case sensitive)")
    void removeEnd_VariousCases_RemovesSuffixCorrectly(String input, String suffix, String expected) {
        assertEquals(expected, StringUtils.removeEnd(input, suffix));
    }

    static Stream<Arguments> removeEndTestCases() {
        return Stream.of(
            Arguments.of("hello.txt", ".txt", "hello"),
            Arguments.of("document.pdf", ".pdf", "document"),
            Arguments.of("file.backup.txt", ".txt", "file.backup"),
            Arguments.of("hello", "lo", "hel"),
            Arguments.of("hello", "HELLO", "hello"), // Case sensitive - no removal
            Arguments.of("HELLO", "LLO", "HE"),
            Arguments.of("", "", ""),
            Arguments.of("test", "", "test"), // Empty suffix
            Arguments.of("test", "test", ""), // Remove entire string
            Arguments.of("abc", "xyz", "abc"), // Suffix not present
            Arguments.of("hello", "hello world", "hello"), // Suffix longer than string
            Arguments.of("aaaa", "aa", "aa") // Remove only from end
        );
    }

    // ========== Tests for removeEndIgnoreCase() ==========

    @Test
    @DisplayName("removeEndIgnoreCase should return null for null string")
    void removeEndIgnoreCase_NullString_ReturnsNull() {
        assertNull(StringUtils.removeEndIgnoreCase(null, "suffix"));
    }

    @Test
    @DisplayName("removeEndIgnoreCase should return original string when suffix is null")
    void removeEndIgnoreCase_NullSuffix_ReturnsOriginal() {
        assertEquals("hello", StringUtils.removeEndIgnoreCase("hello", null));
    }

    @ParameterizedTest
    @MethodSource("removeEndIgnoreCaseTestCases")
    @DisplayName("removeEndIgnoreCase should remove suffix when present (case insensitive)")
    void removeEndIgnoreCase_VariousCases_RemovesSuffixCorrectly(String input, String suffix, String expected) {
        assertEquals(expected, StringUtils.removeEndIgnoreCase(input, suffix));
    }

    static Stream<Arguments> removeEndIgnoreCaseTestCases() {
        return Stream.of(
            Arguments.of("hello.TXT", ".txt", "hello"),
            Arguments.of("document.PDF", ".pdf", "document"),
            Arguments.of("FILE.backup.TXT", ".txt", "FILE.backup"),
            Arguments.of("hello", "LO", "hel"),
            Arguments.of("HELLO", "hello", ""), // Case insensitive - full removal
            Arguments.of("HeLLo", "llo", "He"),
            Arguments.of("", "", ""),
            Arguments.of("test", "", "test"), // Empty suffix
            Arguments.of("ABC", "abc", ""), // Remove entire string
            Arguments.of("abc", "XYZ", "abc"), // Suffix not present
            Arguments.of("hello", "HELLO WORLD", "hello"), // Suffix longer than string
            Arguments.of("AAAA", "aa", "AA") // Remove only from end, case insensitive
        );
    }

    // ========== Tests for prependIfMissing() ==========

    @Test
    @DisplayName("prependIfMissing should return null for null string")
    void prependIfMissing_NullString_ReturnsNull() {
        assertNull(StringUtils.prependIfMissing(null, "prefix"));
    }

    @Test
    @DisplayName("prependIfMissing should return original string when prefix is null")
    void prependIfMissing_NullPrefix_ReturnsOriginal() {
        assertEquals("hello", StringUtils.prependIfMissing("hello", null));
    }

    @ParameterizedTest
    @MethodSource("prependIfMissingTestCases")
    @DisplayName("prependIfMissing should prepend prefix when missing")
    void prependIfMissing_VariousCases_PrependsCorrectly(String input, String prefix, String expected) {
        assertEquals(expected, StringUtils.prependIfMissing(input, prefix));
    }

    static Stream<Arguments> prependIfMissingTestCases() {
        return Stream.of(
            Arguments.of("world", "hello ", "hello world"),
            Arguments.of("hello world", "hello", "hello world"), // Already has prefix
            Arguments.of("", "prefix", "prefix"),
            Arguments.of("test", "", "test"), // Empty prefix
            Arguments.of("http://example.com", "https://", "https://http://example.com"), // Different protocol
            Arguments.of("https://example.com", "https://", "https://example.com"), // Already has prefix
            Arguments.of("/path/to/file", "/", "/path/to/file"), // Already starts with prefix
            Arguments.of("path/to/file", "/", "/path/to/file"), // Needs prefix
            Arguments.of("ABC", "abc", "abcABC"), // Case sensitive
            Arguments.of("abcDEF", "abc", "abcDEF") // Already has prefix
        );
    }

    // ========== Tests for appendIfMissing() ==========

    @Test
    @DisplayName("appendIfMissing should return null for null string")
    void appendIfMissing_NullString_ReturnsNull() {
        assertNull(StringUtils.appendIfMissing(null, "suffix"));
    }

    @Test
    @DisplayName("appendIfMissing should return original string when suffix is null")
    void appendIfMissing_NullSuffix_ReturnsOriginal() {
        assertEquals("hello", StringUtils.appendIfMissing("hello", null));
    }

    @Test
    @DisplayName("appendIfMissing should match Apache Commons Lang3 specification examples")
    void appendIfMissing_ApacheCommonsSpecificationExamples() {
        // Direct examples from Apache Commons Lang3 documentation:
        // StringUtils.appendIfMissing(null, null)      = null
        // StringUtils.appendIfMissing("abc", null)     = "abc"
        // StringUtils.appendIfMissing("", "xyz")       = "xyz"
        // StringUtils.appendIfMissing("abc", "xyz")    = "abcxyz"
        // StringUtils.appendIfMissing("abcxyz", "xyz") = "abcxyz"
        // StringUtils.appendIfMissing("abcXYZ", "xyz") = "abcXYZxyz"
        
        assertNull(StringUtils.appendIfMissing(null, null), "appendIfMissing(null, null) should return null");
        assertEquals("abc", StringUtils.appendIfMissing("abc", null), "appendIfMissing(\"abc\", null) should return \"abc\"");
        assertEquals("xyz", StringUtils.appendIfMissing("", "xyz"), "appendIfMissing(\"\", \"xyz\") should return \"xyz\"");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abc", "xyz"), "appendIfMissing(\"abc\", \"xyz\") should return \"abcxyz\"");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abcxyz", "xyz"), "appendIfMissing(\"abcxyz\", \"xyz\") should return \"abcxyz\"");
        assertEquals("abcXYZxyz", StringUtils.appendIfMissing("abcXYZ", "xyz"), "appendIfMissing(\"abcXYZ\", \"xyz\") should return \"abcXYZxyz\"");
    }

    @Test
    @DisplayName("appendIfMissing should work with additional suffixes")
    void appendIfMissing_WithAdditionalSuffixes() {
        // Examples with additional suffixes from Apache Commons Lang3 documentation:
        // StringUtils.appendIfMissing(null, null, null)       = null
        // StringUtils.appendIfMissing("abc", null, null)      = "abc"
        // StringUtils.appendIfMissing("", "xyz", null)        = "xyz"
        // StringUtils.appendIfMissing("abc", "xyz", new CharSequence[]{null}) = "abcxyz"
        // StringUtils.appendIfMissing("abc", "xyz", "")       = "abc"
        // StringUtils.appendIfMissing("abc", "xyz", "mno")    = "abcxyz"
        // StringUtils.appendIfMissing("abcxyz", "xyz", "mno") = "abcxyz"
        // StringUtils.appendIfMissing("abcmno", "xyz", "mno") = "abcmno"
        // StringUtils.appendIfMissing("abcXYZ", "xyz", "mno") = "abcXYZxyz"
        // StringUtils.appendIfMissing("abcMNO", "xyz", "mno") = "abcMNOxyz"
        
        assertNull(StringUtils.appendIfMissing(null, null, (CharSequence) null), "appendIfMissing(null, null, null) should return null");
        assertEquals("abc", StringUtils.appendIfMissing("abc", null, (CharSequence) null), "appendIfMissing(\"abc\", null, null) should return \"abc\"");
        assertEquals("xyz", StringUtils.appendIfMissing("", "xyz", (CharSequence) null), "appendIfMissing(\"\", \"xyz\", null) should return \"xyz\"");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abc", "xyz", (CharSequence) null), "appendIfMissing(\"abc\", \"xyz\", null) should return \"abcxyz\"");
        assertEquals("abc", StringUtils.appendIfMissing("abc", "xyz", ""), "appendIfMissing(\"abc\", \"xyz\", \"\") should return \"abc\"");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abc", "xyz", "mno"), "appendIfMissing(\"abc\", \"xyz\", \"mno\") should return \"abcxyz\"");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abcxyz", "xyz", "mno"), "appendIfMissing(\"abcxyz\", \"xyz\", \"mno\") should return \"abcxyz\"");
        assertEquals("abcmno", StringUtils.appendIfMissing("abcmno", "xyz", "mno"), "appendIfMissing(\"abcmno\", \"xyz\", \"mno\") should return \"abcmno\"");
        assertEquals("abcXYZxyz", StringUtils.appendIfMissing("abcXYZ", "xyz", "mno"), "appendIfMissing(\"abcXYZ\", \"xyz\", \"mno\") should return \"abcXYZxyz\"");
        assertEquals("abcMNOxyz", StringUtils.appendIfMissing("abcMNO", "xyz", "mno"), "appendIfMissing(\"abcMNO\", \"xyz\", \"mno\") should return \"abcMNOxyz\"");
    }

    @ParameterizedTest
    @MethodSource("appendIfMissingTestCases")
    @DisplayName("appendIfMissing should append suffix when missing")
    void appendIfMissing_VariousCases_AppendsCorrectly(String input, String suffix, String expected) {
        assertEquals(expected, StringUtils.appendIfMissing(input, suffix));
    }

    static Stream<Arguments> appendIfMissingTestCases() {
        return Stream.of(
            Arguments.of("hello", " world", "hello world"),
            Arguments.of("hello world", "world", "hello world"), // Already has suffix
            Arguments.of("", "suffix", "suffix"),
            Arguments.of("test", "", "test"), // Empty suffix
            Arguments.of("file", ".txt", "file.txt"),
            Arguments.of("file.txt", ".txt", "file.txt"), // Already has suffix
            Arguments.of("path/to/dir", "/", "path/to/dir/"), // Needs trailing slash
            Arguments.of("path/to/dir/", "/", "path/to/dir/"), // Already has trailing slash
            Arguments.of("ABC", "abc", "ABCabc"), // Case sensitive
            Arguments.of("ABCabc", "abc", "ABCabc") // Already has suffix
        );
    }

    @Test
    @DisplayName("appendIfMissing should work with different CharSequence types")
    void appendIfMissing_DifferentCharSequenceTypes() {
        // Test with StringBuilder as suffix
        StringBuilder sb = new StringBuilder("xyz");
        assertEquals("abcxyz", StringUtils.appendIfMissing("abc", sb));
        assertEquals("abcxyz", StringUtils.appendIfMissing("abcxyz", sb));
        
        // Test with StringBuffer as suffix
        StringBuffer sbuf = new StringBuffer("mno");
        assertEquals("abcmno", StringUtils.appendIfMissing("abc", sbuf));
        assertEquals("abcmno", StringUtils.appendIfMissing("abcmno", sbuf));
        
        // Test with multiple CharSequence types as additional suffixes
        assertEquals("abcxyz", StringUtils.appendIfMissing("abcxyz", "abc", sb, sbuf));
        assertEquals("abcmno", StringUtils.appendIfMissing("abcmno", "abc", sb, sbuf));
        assertEquals("abcpqr", StringUtils.appendIfMissing("abc", "pqr", sb, sbuf));
    }

    // ========== Tests for endsWithIgnoreCase() ==========

    @Test
    @DisplayName("endsWithIgnoreCase should return true when both string and suffix are null")
    void endsWithIgnoreCase_BothNull_ReturnsTrue() {
        assertTrue(StringUtils.endsWithIgnoreCase(null, null));
    }

    @Test
    @DisplayName("endsWithIgnoreCase should return false for null string with non-null suffix")
    void endsWithIgnoreCase_NullString_ReturnsFalse() {
        assertFalse(StringUtils.endsWithIgnoreCase(null, "suffix"));
    }

    @Test
    @DisplayName("endsWithIgnoreCase should return false when suffix is null but string is not")
    void endsWithIgnoreCase_NullSuffix_ReturnsFalse() {
        assertFalse(StringUtils.endsWithIgnoreCase("hello", null));
    }

    @ParameterizedTest
    @MethodSource("endsWithIgnoreCaseTestCases")
    @DisplayName("endsWithIgnoreCase should check suffix presence (case insensitive)")
    void endsWithIgnoreCase_VariousCases_ChecksCorrectly(String input, String suffix, boolean expected) {
        assertEquals(expected, StringUtils.endsWithIgnoreCase(input, suffix));
    }

    @Test
    @DisplayName("endsWithIgnoreCase should match Apache Commons Lang3 specification examples")
    void endsWithIgnoreCase_ApacheCommonsSpecificationExamples() {
        // examples from Apache Commons Lang3 documentation:
        // StringUtils.endsWithIgnoreCase(null, null)      = true
        // StringUtils.endsWithIgnoreCase(null, "def")     = false
        // StringUtils.endsWithIgnoreCase("abcdef", null)  = false
        // StringUtils.endsWithIgnoreCase("abcdef", "def") = true
        // StringUtils.endsWithIgnoreCase("ABCDEF", "def") = true
        // StringUtils.endsWithIgnoreCase("ABCDEF", "cde") = false
        
        assertTrue(StringUtils.endsWithIgnoreCase(null, null), "endsWithIgnoreCase(null, null) should return true");
        assertFalse(StringUtils.endsWithIgnoreCase(null, "def"), "endsWithIgnoreCase(null, \"def\") should return false");
        assertFalse(StringUtils.endsWithIgnoreCase("abcdef", null), "endsWithIgnoreCase(\"abcdef\", null) should return false");
        assertTrue(StringUtils.endsWithIgnoreCase("abcdef", "def"), "endsWithIgnoreCase(\"abcdef\", \"def\") should return true");
        assertTrue(StringUtils.endsWithIgnoreCase("ABCDEF", "def"), "endsWithIgnoreCase(\"ABCDEF\", \"def\") should return true");
        assertFalse(StringUtils.endsWithIgnoreCase("ABCDEF", "cde"), "endsWithIgnoreCase(\"ABCDEF\", \"cde\") should return false");
    }

    @Test
    @DisplayName("endsWithIgnoreCase should work with different CharSequence types")
    void endsWithIgnoreCase_DifferentCharSequenceTypes() {
        // Test with String
        assertTrue(StringUtils.endsWithIgnoreCase("hello.TXT", ".txt"));
        
        // Test with StringBuilder
        StringBuilder sb = new StringBuilder("document.PDF");
        assertTrue(StringUtils.endsWithIgnoreCase(sb, ".pdf"));
        
        StringBuilder suffix = new StringBuilder(".HTML");
        assertTrue(StringUtils.endsWithIgnoreCase("index.html", suffix));
        
        // Test with StringBuffer
        StringBuffer sbuf = new StringBuffer("file.XML");
        assertTrue(StringUtils.endsWithIgnoreCase(sbuf, ".xml"));
        
        // Test with mixed types
        StringBuffer str = new StringBuffer("test.JSON");
        StringBuilder suf = new StringBuilder(".json");
        assertTrue(StringUtils.endsWithIgnoreCase(str, suf));
    }

    static Stream<Arguments> endsWithIgnoreCaseTestCases() {
        return Stream.of(
            Arguments.of("hello", "lo", true),
            Arguments.of("hello", "LO", true),
            Arguments.of("HELLO", "lo", true),
            Arguments.of("hello", "hi", false),
            Arguments.of("hello", "HI", false),
            Arguments.of("file.txt", ".TXT", true),
            Arguments.of("FILE.TXT", ".txt", true),
            Arguments.of("document.pdf", ".PDF", true),
            Arguments.of("image.jpg", ".png", false),
            Arguments.of("", "", true),
            Arguments.of("test", "", true),
            Arguments.of("", "test", false),
            Arguments.of("a", "a", true),
            Arguments.of("A", "a", true),
            Arguments.of("long string", "STRING", true),
            Arguments.of("long string", "String", true),
            Arguments.of("case sensitive", "SENSITIVE", true)
        );
    }

    // ========== Integration Tests ==========

    @Test
    @DisplayName("Test chaining multiple utility methods")
    void integrationTest_ChainingMethods_WorksCorrectly() {
        String input = "  hello.TXT  ";
        
        // Test chaining: trim -> removeEndIgnoreCase -> prependIfMissing
        String trimmed = input.trim();
        String withoutExt = StringUtils.removeEndIgnoreCase(trimmed, ".txt");
        String withPrefix = StringUtils.prependIfMissing(withoutExt, "greeting_");
        
        assertEquals("greeting_hello", withPrefix);
    }

    @Test
    @DisplayName("Test utility methods with real-world file path scenarios")
    void integrationTest_FilePathScenarios_WorksCorrectly() {
        // Scenario: Processing file paths
        String filepath = "/home/user/document.PDF";
        
        assertTrue(StringUtils.endsWithIgnoreCase(filepath, ".pdf"));
        assertEquals("/home/user/document", StringUtils.removeEndIgnoreCase(filepath, ".PDF"));
        
        String newPath = StringUtils.appendIfMissing(StringUtils.removeEndIgnoreCase(filepath, ".pdf"), ".txt");
        assertEquals("/home/user/document.txt", newPath);
    }

    @Test
    @DisplayName("Test utility methods with URL scenarios")
    void integrationTest_UrlScenarios_WorksCorrectly() {
        // Scenario: Processing URLs
        String url = "example.com/api/data";
        
        String httpsUrl = StringUtils.prependIfMissing(url, "https://");
        assertEquals("https://example.com/api/data", httpsUrl);
        
        String urlWithSlash = StringUtils.appendIfMissing(httpsUrl, "/");
        assertEquals("https://example.com/api/data/", urlWithSlash);
        
        assertFalse(StringUtils.isBlank(urlWithSlash));
        assertTrue(StringUtils.isNotBlank(urlWithSlash));
    }

    @Test
    @DisplayName("Test edge cases and boundary conditions")
    void edgeCases_BoundaryConditions_HandledCorrectly() {
        // Test with special characters
        String specialChars = "!@#$%^&*()";
        assertFalse(StringUtils.isBlank(specialChars));
        assertTrue(StringUtils.isNotBlank(specialChars));
        assertFalse(StringUtils.isEmpty(specialChars));
        
        // Test with unicode characters
        String unicode = "Hello 世界 🌍";
        assertEquals("Hello 世界 🌍.txt", StringUtils.appendIfMissing(unicode, ".txt"));
        assertEquals("prefix_Hello 世界 🌍", StringUtils.prependIfMissing(unicode, "prefix_"));
        
        // Test very long strings
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("a");
        }
        String longString = sb.toString();
        assertEquals(999, StringUtils.chop(longString).length());
        assertTrue(StringUtils.endsWithIgnoreCase(longString, "A"));
    }



    // ========== Tests for unescapeJava ==========

    @Test
    @DisplayName("unescapeJava should handle null input")
    void unescapeJava_NullInput_ReturnsNull() {
        assertNull(StringUtils.unescapeJava(null));
    }

    @Test
    @DisplayName("unescapeJava should handle empty string")
    void unescapeJava_EmptyString_ReturnsEmpty() {
        assertEquals("", StringUtils.unescapeJava(""));
    }

    @Test
    @DisplayName("unescapeJava should handle string without escapes")
    void unescapeJava_NoEscapes_ReturnsOriginal() {
        String input = "Hello World 123";
        assertEquals(input, StringUtils.unescapeJava(input));
    }

    @Test
    @DisplayName("unescapeJava should handle basic escape sequences")
    void unescapeJava_BasicEscapes_UnescapesCorrectly() {
        assertEquals("\\", StringUtils.unescapeJava("\\\\"));
        assertEquals("\"", StringUtils.unescapeJava("\\\""));
        assertEquals("'", StringUtils.unescapeJava("\\'"));
        assertEquals("\n", StringUtils.unescapeJava("\\n"));
        assertEquals("\r", StringUtils.unescapeJava("\\r"));
        assertEquals("\t", StringUtils.unescapeJava("\\t"));
        assertEquals("\b", StringUtils.unescapeJava("\\b"));
        assertEquals("\f", StringUtils.unescapeJava("\\f"));
    }

    @Test
    @DisplayName("unescapeJava should handle complex strings with multiple escapes")
    void unescapeJava_MultipleEscapes_UnescapesCorrectly() {
        String input = "Line1\\nLine2\\tTabbed\\r\\nWindows line end\\\\Backslash\\\"Quote";
        String expected = "Line1\nLine2\tTabbed\r\nWindows line end\\Backslash\"Quote";
        assertEquals(expected, StringUtils.unescapeJava(input));
    }

    @Test
    @DisplayName("unescapeJava should handle Unicode escape sequences")
    void unescapeJava_UnicodeEscapes_UnescapesCorrectly() {
        assertEquals("A", StringUtils.unescapeJava("\\u0041")); // 'A'
        assertEquals("€", StringUtils.unescapeJava("\\u20AC")); // Euro symbol
        assertEquals("😀", StringUtils.unescapeJava("\\uD83D\\uDE00")); // Emoji (surrogate pair)
        assertEquals("Hello © World", StringUtils.unescapeJava("Hello \\u00A9 World")); // Copyright symbol
    }

    @Test
    @DisplayName("unescapeJava should handle octal escape sequences")
    void unescapeJava_OctalEscapes_UnescapesCorrectly() {
        assertEquals("A", StringUtils.unescapeJava("\\101")); // Octal 101 = decimal 65 = 'A'
        assertEquals("@", StringUtils.unescapeJava("\\100")); // Octal 100 = decimal 64 = '@'
        assertEquals("\0", StringUtils.unescapeJava("\\0")); // Null character
        assertEquals("7", StringUtils.unescapeJava("\\67")); // Octal 67 = decimal 55 = '7'
    }

    @Test
    @DisplayName("unescapeJava should handle invalid escape sequences")
    void unescapeJava_InvalidEscapes_TreatsAsLiteral() {
        assertEquals("\\x", StringUtils.unescapeJava("\\x")); // Invalid escape
        assertEquals("\\z", StringUtils.unescapeJava("\\z")); // Invalid escape
        assertEquals("\\", StringUtils.unescapeJava("\\")); // Trailing backslash
        assertEquals("\\uXXXX", StringUtils.unescapeJava("\\uXXXX")); // Invalid unicode
        assertEquals("\\u12", StringUtils.unescapeJava("\\u12")); // Incomplete unicode
    }

    @Test
    @DisplayName("unescapeJava should handle edge cases")
    void unescapeJava_EdgeCases_HandlesCorrectly() {
        // Multiple backslashes
        assertEquals("\\\\", StringUtils.unescapeJava("\\\\\\\\"));
        
        // Mixed valid and invalid escapes
        assertEquals("\\x\n\\y", StringUtils.unescapeJava("\\x\\n\\y"));
        
        // Octal with maximum 3 digits
        assertEquals("A4", StringUtils.unescapeJava("\\1014")); // Should parse \\101 and leave '4'
        
        // Unicode at end of string
        assertEquals("A", StringUtils.unescapeJava("\\u0041"));
        
        // Invalid octal values
        assertEquals("\\999", StringUtils.unescapeJava("\\999")); // 999 > 255, should be treated as literal
    }

    @Test
    @DisplayName("unescapeJava should handle real-world JSON-like strings")
    void unescapeJava_JsonLikeStrings_UnescapesCorrectly() {
        String jsonString = "{\\\"name\\\":\\\"John\\\",\\\"age\\\":30,\\\"city\\\":\\\"New York\\\"}";
        String expected = "{\"name\":\"John\",\"age\":30,\"city\":\"New York\"}";
        assertEquals(expected, StringUtils.unescapeJava(jsonString));
    }

    @Test
    @DisplayName("unescapeJava should match Apache Commons Lang3 behavior")
    void unescapeJava_ApacheCommonsLang3Compatibility_BehavesCorrectly() {
        // Test cases based on Apache Commons Lang3 StringEscapeUtils.unescapeJava() specification
        assertEquals("\"", StringUtils.unescapeJava("\\\""));
        assertEquals("\\", StringUtils.unescapeJava("\\\\"));
        assertEquals("\n", StringUtils.unescapeJava("\\n"));
        assertEquals("\t", StringUtils.unescapeJava("\\t"));
        assertEquals("\r", StringUtils.unescapeJava("\\r"));
        assertEquals("\b", StringUtils.unescapeJava("\\b"));
        assertEquals("\f", StringUtils.unescapeJava("\\f"));
        assertEquals("'", StringUtils.unescapeJava("\\'"));
        assertEquals("/", StringUtils.unescapeJava("\\/")); // Should treat as literal
        
        // Complex test case
        String complex = "He didn't say, \\\"Stop!\\\"";
        String expectedComplex = "He didn't say, \"Stop!\"";
        assertEquals(expectedComplex, StringUtils.unescapeJava(complex));
    }


}
