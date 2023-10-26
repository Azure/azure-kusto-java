package com.microsoft.azure.kusto.ingest.resources;

import com.microsoft.azure.kusto.ingest.MockTimeProvider;
import com.microsoft.azure.kusto.ingest.utils.RandomProvider;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class RankedStorageAccountSetTest {

    class ByNameReverseOrderRandomProvider implements RandomProvider {
        @Override
        public void shuffle(List<?> list) {
            list.sort((o1, o2) -> ((RankedStorageAccount) o2).getAccountName().compareTo(((RankedStorageAccount) o1).getAccountName()));
        }
    }

    @Test
    public void testShuffledAccountsNoTiers() {
        int[] ints = new int[] {0};

        RankedStorageAccountSet rankedStorageAccountSet = new RankedStorageAccountSet(
                6,
                10,
                ints,
                new MockTimeProvider(System.currentTimeMillis()),
                new ByNameReverseOrderRandomProvider());

        rankedStorageAccountSet.addAccount("aSuccessful");
        rankedStorageAccountSet.addAccount("bFailed");
        rankedStorageAccountSet.addAccount("cHalf");

        rankedStorageAccountSet.addResultToAccount("aSuccessful", true);
        rankedStorageAccountSet.addResultToAccount("bFailed", false);
        rankedStorageAccountSet.addResultToAccount("cHalf", true);
        rankedStorageAccountSet.addResultToAccount("cHalf", false);

        List<RankedStorageAccount> accounts = rankedStorageAccountSet.getRankedShuffledAccounts();
        assertEquals("cHalf", accounts.get(0).getAccountName());
        assertEquals("bFailed", accounts.get(1).getAccountName());
        assertEquals("aSuccessful", accounts.get(2).getAccountName());
    }

    @Test
    public void testShuffledAccounts() {
        int[] tiers = new int[] {90, 70, 30, 0};

        RankedStorageAccountSet rankedStorageAccountSet = new RankedStorageAccountSet(
                6,
                10,
                tiers,
                new MockTimeProvider(System.currentTimeMillis()),
                new ByNameReverseOrderRandomProvider());

        int[] values = new int[] {95, 40, 80, 20, 97, 10, 50, 75, 29, 0};
        for (int i = 0; i < values.length; i++) {
            String name = String.format("%s%d", (char) ('a' + i), values[i]);
            rankedStorageAccountSet.addAccount(name);
            for (int j = 0; j < 100; j++) {
                rankedStorageAccountSet.addResultToAccount(name, j < values[i]);
            }
        }

        String[][] expected = new String[][] {
                {"e97", "a95"},
                {"h75", "c80"},
                {"g50", "b40"},
                {"j0", "i29", "f10", "d20"}
        };

        List<RankedStorageAccount> accounts = rankedStorageAccountSet.getRankedShuffledAccounts();
        int total = 0;
        for (String[] strings : expected) {
            for (int j = 0; j < strings.length; j++) {
                assertEquals(strings[j], accounts.get(total + j).getAccountName());
            }
            total += strings.length;
        }
    }

    @Test
    public void testShuffledAccountsEmptyTier() {
        int[] tiers = new int[] {90, 70, 30, 0};

        RankedStorageAccountSet rankedStorageAccountSet = new RankedStorageAccountSet(
                6,
                10,
                tiers,
                new MockTimeProvider(System.currentTimeMillis()),
                new ByNameReverseOrderRandomProvider());

        int[] values = new int[] {95, 40, 20, 97, 10, 50};
        for (int i = 0; i < values.length; i++) {
            String name = String.format("%s%d", (char) ('a' + i), values[i]);
            rankedStorageAccountSet.addAccount(name);
            for (int j = 0; j < 100; j++) {
                rankedStorageAccountSet.addResultToAccount(name, j < values[i]);
            }
        }

        String[][] expected = new String[][] {
                {"d97", "a95"},
                {"f50", "b40"},
                {"e10", "c20"}
        };

        List<RankedStorageAccount> accounts = rankedStorageAccountSet.getRankedShuffledAccounts();
        int total = 0;
        for (String[] strings : expected) {
            for (int j = 0; j < strings.length; j++) {
                assertEquals(strings[j], accounts.get(total + j).getAccountName());
            }
            total += strings.length;
        }
    }

}
