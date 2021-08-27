#include <iostream>
#include <fstream>
#include <random>
#include <thread>
#include <algorithm>
#include <mutex>
#include <memory>
#include <functional>
#include <climits>

// input file size in bytes 
constexpr int generatedFileSize = 200 * 1024 * 1024 + 1024; 

// command line arguments count
constexpr int maxArgumentsCount = 3;

// read segment size
constexpr int segmentSize = 8 * 1024 * 1024;

// max threads count
constexpr int maxThreadsCount = 10;

// generate input file
int generateBinaryFile(const std::string& fileName);

// read values from input file and add them to vector
void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, long long startPosition, long long segment);

// get total file length
const long long getFileLength(const std::string& fileName);

// merge sorted vectors
std::vector<unsigned> mergeSortedVectors(const std::vector<unsigned>& v1, const std::vector<unsigned>& v2);

// sort subsequence
void sortSubsequence(std::vector<unsigned>& result, const std::vector<std::vector<unsigned>>& values, size_t left, size_t right);

int main(int argc, char* argv[])
{
   auto begin = std::chrono::steady_clock::now();

   try
   {
      if (argc != maxArgumentsCount)
      {
         throw std::logic_error("Incorrect arguments count");
      }

      /*if (generateBinaryFile(argv[1]))
      {
         throw std::logic_error("Can't open input binary file to write random values.");
      }*/
 
      const long long fileLenght = getFileLength(argv[1]);
      const long long size = fileLenght % segmentSize == 0 ? fileLenght / segmentSize : fileLenght / segmentSize + 1;
      std::vector<std::vector<unsigned>> values(size);
      int counter = 0;
      long long startPosition = 0;
      while (startPosition < fileLenght)
      {
         std::vector<std::unique_ptr<std::thread>> threads;
         for (int i = 0; i < maxThreadsCount; ++i)
         {
            if (startPosition >= fileLenght)
            {
               break;
            }
            const long long segment = fileLenght - startPosition >= segmentSize ? segmentSize : fileLenght - startPosition;
            threads.emplace_back(std::make_unique<std::thread>(std::bind(&readValuesFromFile, std::cref(argv[1]), std::ref(values[counter]), startPosition, segment)));
            startPosition += segment;
            counter++;
         }

         for (auto& vp : threads)
         {
            if (vp->joinable())
            {
               vp->join();
            }
         }
      }
      
      counter = 0;
      while (counter < size)
      {
         std::vector<std::unique_ptr<std::thread>> threads;
         for (int i = 0; i < maxThreadsCount; ++i)
         {
            if (counter == size)
            {
               break;
            }
            threads.emplace_back(std::make_unique<std::thread>(std::bind([&vec = values[counter]](){std::sort(vec.begin(), vec.end()); })));
            counter++;
         }

         for (auto& vp : threads)
         {
            if (vp->joinable())
            {
               vp->join();
            }
         }
      }

      size_t start = 0;
      size_t middle = values.size() / 2;
      size_t end = values.size();
      
      std::vector<unsigned> left(0);
      std::vector<unsigned> right(0);
      std::thread letfThread(&sortSubsequence, std::ref(left), std::cref(values), start, middle);
      std::thread rightThread(&sortSubsequence, std::ref(right), std::cref(values), middle, end);

      letfThread.join();
      rightThread.join();

      std::vector<unsigned> result = mergeSortedVectors(left, right);
      std::ofstream out(argv[2]);
      if (!out.is_open())
      {
         throw std::ios::failure("Can't open output file to write sorted values.");
      }
      //std::copy(std::begin(result), std::end(result), std::ostream_iterator<int>(out, " "));
      out.write(reinterpret_cast<char*>(result.data()), sizeof(unsigned) * result.size());
      out.close();
   }
   catch (const std::exception& ex)
   {
      std::cerr << ex.what() << std::endl;
      return EXIT_FAILURE;
   }
   catch (...)
   {
      std::cerr << "Something goes wrong." << std::endl;
      return EXIT_FAILURE;
   }

   auto end = std::chrono::steady_clock::now();
   auto seconds = std::chrono::duration_cast<std::chrono::seconds>(end - begin);
   std::cout << "Total time: " << seconds.count() << std::endl;
  
   return EXIT_SUCCESS;
}

int generateBinaryFile(const std::string& fileName)
{
   std::fstream out(fileName, std::ios::binary | std::ios::out);
   if (out.is_open())
   {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<unsigned> dist(0, USHRT_MAX);
      for (int i = 0; i < generatedFileSize / sizeof(unsigned); ++i)
      {
         unsigned value = dist(mt);
         out.write(reinterpret_cast<char*>(&value), sizeof(unsigned));
      }
      out.close();
      return 0;
   }
   else
   {
      return 1;
   }
}

void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, long long startPosition, long long segment)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (!in.is_open())
   {
      throw std::logic_error("Can't open input file to read values.");
   }

   in.seekg(startPosition);
   for (int i = 0; i < segment / sizeof(unsigned); ++i)
   {
      unsigned val;
      in.read(reinterpret_cast<char*>(&val), sizeof(unsigned));
      values.push_back(val);
   }
   std::cout << std::endl;
   in.close();
}

const long long getFileLength(const std::string& fileName)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (!in.is_open())
   {
      throw std::logic_error("Can't open input file to get file length.");
   }

   in.seekg(0, in.end);
   const long long length = in.tellg();
   in.seekg(0, in.beg);
   in.close();
   return length;
}

std::vector<unsigned> mergeSortedVectors(const std::vector<unsigned>& v1, const std::vector<unsigned>& v2)
{
   std::vector<unsigned> result;
   std::merge(v1.begin(), v1.end(), v2.begin(), v2.end(), std::back_inserter(result));
   return result;
}

void sortSubsequence(std::vector<unsigned>& result, const std::vector<std::vector<unsigned>>& values, size_t left, size_t right)
{
   for (size_t i = left; i < right; ++i)
   {
      result = mergeSortedVectors(result, values[i]);
   }
}