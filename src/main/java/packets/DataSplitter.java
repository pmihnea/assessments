package packets;

import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Optional;

/*
    Split the data in packets
 */
public class DataSplitter {
    /*
        The split should fulfill these rules:
    1) return the minimum number of packets
     */
    public List<byte[]> splitData1(byte[] data, int packetDataSize) {
        //check parameters
        if (data == null || data.length == 0) return Collections.emptyList();
        if (packetDataSize <= 0) throw new IllegalArgumentException("illegal packet data size");

        // split data
        List<byte[]> result = new LinkedList<>();
        int index = 0;
        while (index < data.length) {
            final int end = index + packetDataSize;
            byte[] packet = Arrays.copyOfRange(data, index, end < data.length ? end : data.length);
            result.add(packet);
        }
        return result;
    }

    /*
    The split should fulfill these rules:
    1) return the minimum number of packets
    2) the largest packet should have the smallest possible size
     */
    public List<byte[]> splitData2(byte[] data, int packetDataSize) {
        //check parameters
        if (data == null || data.length == 0) return Collections.emptyList();
        if (packetDataSize <= 0) throw new IllegalArgumentException("illegal packet data size");

        // split data
        int noPackets = data.length / packetDataSize;
        int rest = data.length % packetDataSize;
        if (rest > 0) noPackets += 1;
        int minPacketSize = data.length / noPackets;
        List<byte[]> result = new LinkedList<>();
        int index = 0;
        while (index < data.length) {
            int packetSize = minPacketSize;
            if (rest > 0) {
                packetSize += 1;
                rest -= 1;
            }
            final int end = index + packetSize;
            byte[] packet = Arrays.copyOfRange(data, index, end < data.length ? end : data.length);
            result.add(packet);
            index += packetSize;
        }
        return result;
    }

    /*
    The split should fulfill these rules:
    1) return the minimum number of packets
    2) the largest packet should have the smallest possible size
    3) use the given split positions if possible

    Rules consequences:
    1) nPackets = data.length + packetDataSize - 1 / packetDataSize
    2) maxPacketSize, maxPacketSize
        rest = data.length % packetDataSize
        if( rest == 0 ) maxPacketSize = minPacketSize = data.length / packetDataSize
        else
            minPacketSize = data.length / packetDataSize
            maxPacketSize = minPacketSize + 1
            nMaxPackets = rest
            nMinPackets = nPackets - rest
    3) The only variation in split positions is to interleave packets with max size with those with min size
    to fit with the given split positions
     */
    private List<List<Integer>> filterPositions(int[] splitPositions, int currentPosition, int currentSize,
                                                Deque<Integer> acc, List<List<Integer>> result,
                                                int minPacketSize, int nMinPackets, int maxPacketSize, int nMaxPackets) {
        if (currentPosition == splitPositions.length) result.add(new LinkedList<>(acc));
        else {
            final int splitPosition = splitPositions[currentPosition];

            final boolean fitsMinPacket = (splitPosition - currentSize) == minPacketSize && nMinPackets > 0;
            final boolean fitsMaxPacket = (splitPosition - currentSize) == maxPacketSize && nMaxPackets > 0;
            final boolean isTooSmall = (splitPosition - currentSize) < minPacketSize
                    && (nMinPackets > 0 || nMaxPackets > 0);

            if (fitsMinPacket) {
                acc.addLast(splitPosition);
                filterPositions(splitPositions, currentPosition + 1, splitPosition,
                        acc, result,
                        minPacketSize, nMinPackets - 1, maxPacketSize, nMaxPackets);
                acc.removeLast();
            }

            if (fitsMaxPacket) {
                acc.addLast(splitPosition);
                filterPositions(splitPositions, currentPosition + 1, splitPosition,
                        acc, result,
                        minPacketSize, nMinPackets, maxPacketSize, nMaxPackets - 1);
                acc.removeLast();
            }

            if (isTooSmall) {
                filterPositions(splitPositions, currentPosition + 1, splitPosition,
                        acc, result,
                        minPacketSize, nMinPackets, maxPacketSize, nMaxPackets);
            } else {
                // suppose that the given split positions are sorted ascending
                // it doesn't make sense to continue with the next position as the current one is already too big
                // return the current positions
                if (!acc.isEmpty()) result.add(new LinkedList<>(acc));
            }
        }
        return result;
    }

    public List<byte[]> splitData3(byte[] data, int packetDataSize, int[] splitPositions) {
        int nPackets = (data.length + packetDataSize - 1) / packetDataSize;
        int maxPacketSize, minPacketSize;
        int rest = data.length % packetDataSize;
        if (rest == 0) {
            maxPacketSize = minPacketSize = data.length / packetDataSize;
        } else {
            minPacketSize = data.length / packetDataSize;
            maxPacketSize = minPacketSize + 1;
        }
        int nMaxPackets = rest;
        int nMinPackets = nPackets - rest;

        final List<List<Integer>> filteredSplitPositions = filterPositions(splitPositions, 0, 0,
                new LinkedList<>(), new LinkedList<>(),
                minPacketSize, nMinPackets,
                maxPacketSize, nMaxPackets);
        final Optional<List<Integer>> bestSplitPositions = filteredSplitPositions.stream().max(Comparator.comparing(List::size));

        if (bestSplitPositions.isEmpty()) return splitData2(data, packetDataSize);
        else {
            List<byte[]> result = new LinkedList<>();
            int index = 0;
            // use the best split positions
            final List<Integer> finalSplitPositions = bestSplitPositions.get();
            final Iterator<Integer> positionIterator = finalSplitPositions.iterator();
            while(index < data.length && positionIterator.hasNext()){
                int position = positionIterator.next();
                if (position == 0) continue;
                if(position - index == minPacketSize) nMinPackets--;
                else if(position - index == maxPacketSize) nMaxPackets--;
                result.add(Arrays.copyOfRange(data, index, position));
                index = position;
            }
            // split the rest
            while(index < data.length) {
                if(nMaxPackets > 0){
                    result.add(Arrays.copyOfRange(data, index, index + maxPacketSize));
                    index += maxPacketSize;
                    nMaxPackets--;
                }else if(nMinPackets > 0){
                    result.add(Arrays.copyOfRange(data, index, index + minPacketSize));
                    index += minPacketSize;
                    nMinPackets--;
                }else{
                    throw new IllegalStateException("wrong min/max packets calculation");
                }
            }

            return result;
        }
    }

    /*
    There are N total packets, K with minPacketSize and N-K with minPacketSize+1
    and the packets have the order index from 1 to N.
    The problem can be reduces to a combination of K indexes out of N = Comb(N,K),
    so the given split positions should represent a Combination(N,P) where 1<=P<=K
     */
}
