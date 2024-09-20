
// import io

// from math import log
// from typing import Tuple, List, BinaryIO, AnyStr

// frequency = None

// # The minimum partition size should be 256 bytes as the keyspace
// # for a char is 256 bytes
// MIN_PARTITION_SIZE = 256




// def calculate_entropy(contents: bytes) -> float:
//     """ this function calculates the entropy of the file
//         It is given by the formula:
//             E = -SUM[v in 0..255](p(v) * ln(p(v)))
//     """
//     calculator = BufferedCalculator()
//     calculator.update(contents)
//     return calculator.entropy()


// def calculate_partition_entropy(fin: BinaryIO, num_partitions: int = 50) -> Tuple[float, List[float]]:
//     """Calculate the entropy of a file and its partitions."""

//     # Split input into num_parititions and calculate
//     # parition entropy.
//     fin.seek(0, io.SEEK_END)
//     size = fin.tell()
//     fin.seek(0)

//     if size == 0:
//         return 0, [0]

//     # Calculate the partition size to get the desired amount of partitions but make sure those
//     # partitions are the minimum partition size
//     partition_size = max((size - 1)//num_partitions + 1, MIN_PARTITION_SIZE)

//     # If our calculated partition size is the minimum partition size, our files is likely too small we will
//     # calculate an alternate partition size that will make sure all blocks are equal size
//     if partition_size == MIN_PARTITION_SIZE:
//         partition_size = (size-1) // ((size-1)//partition_size + 1) + 1

//     # Also calculate full file entropy using buffered calculator.
//     p_entropies = []
//     full_entropy_calculator = BufferedCalculator()
//     for _ in range(num_partitions):
//         partition = fin.read(partition_size)
//         if not partition:
//             break
//         p_entropies.append(calculate_entropy(partition))
//         full_entropy_calculator.update(partition)
//     return full_entropy_calculator.entropy(), p_entropies


pub struct BufferedCalculator {
    counts: Box<[u64; 256]>,
    length: u64,
}

impl BufferedCalculator {
    pub fn new() -> Self {
        Self {
            counts: Box::new([0; 256]),
            length: 0,
        }
    }

    pub fn entropy(&self) -> f64 {
        if self.length == 0 {
            return 0.0;
        }

        let length = self.length as f64;

        let mut entropy = 0.0;
        for v in self.counts.iter() {
            let prob = *v as f64 / length;
            if prob != 0.0 {
                entropy += prob * prob.log2();
            }
        }

        entropy *= -1.0;

        // Make sure we don't return -0.0. Just keep things pretty.
        if entropy == -0.0 {
            entropy = 0.0;
        }

        return entropy

    }

    pub fn update(&mut self, data: &[u8]) {
        self.length += data.len() as u64;
        for byte in data {
            self.counts[*byte as usize] += 1;
        }        
    }

//     def update(self, data: AnyStr, length: int = 0):
//         if not length:
//             length = len(data)

//         self.length += length
//         self.c = frequency.counts(data, length, self.c)
}

// def counts(b, c, d=None):
//     if d is None:
//         d = {}
//     cdef long long t[256]
//     cdef unsigned char* s = b
//     cdef int l = c
//     cdef int i = 0

//     memset(t, 0, 256 * sizeof(long long))

//     for k, v in d.iteritems():
//         t[k] = v

//     while i < l:
//         t[s[i]] += 1
//         i += 1

//     return {i: t[i] for i in range(256) if t[i]}