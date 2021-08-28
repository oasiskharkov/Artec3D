#include <iostream>
#include <fstream>
#include <random>
#include <thread>
#include <algorithm>
#include <mutex>
#include <memory>
#include <functional>
#include <climits>
#include <chrono>

// input binary file size in bytes 
constexpr int generatedFileSize = 200 * 1024 * 1024 + 1024; 

// command line arguments count
constexpr int maxArgumentsCount = 3;

// read segment size
constexpr int segmentSize = 32 * 1024 * 1024;

// max threads count
constexpr int maxThreadsCount = 8;

// generate input file
int generateBinaryFile(const std::string& fileName);

// read values from input file and add them to vector
void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, const long long startPosition, const long long segment);

// get total file length
const long long getFileLength(const std::string& fileName);

// merge two sorted vectors
std::vector<unsigned> mergeSortedVectors(const std::vector<unsigned>& v1, const std::vector<unsigned>& v2);

// fill vectors from input binary file
void fillVectorsFromFile(const std::string& fileName, std::vector<std::vector<unsigned>>& values, const long long fileLength);

// sort subsequence
void sortSubsequence(std::vector<unsigned>& result, const std::vector<std::vector<unsigned>>& values, const size_t left, const size_t right);

// write sorted values to binary file
void writeSortedVectorToFile(const std::string& fileName, const std::vector<unsigned>& result);

// merge all sorted vectors to result sorted vector
void mergeAllSortedVectors(std::vector<unsigned>& result, std::vector<std::vector<unsigned>>& values);

// sort all vectors
void sortVectors(std::vector<std::vector<unsigned>>& values);

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

      const long long fileLength = getFileLength(argv[1]);
      if (!fileLength)
      {
         throw std::logic_error("Input file is empty.");
      }

      const long long size = fileLength % segmentSize == 0 ? fileLength / segmentSize : fileLength / segmentSize + 1;
      std::vector<std::vector<unsigned>> values(size);
      fillVectorsFromFile(argv[1], values, fileLength);

      sortVectors(values);

      std::vector<unsigned> result;
      mergeAllSortedVectors(result, values);

      writeSortedVectorToFile(argv[2], result);
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
      return EXIT_SUCCESS;
   }
   return EXIT_FAILURE;
}

void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, const long long startPosition, const long long segment)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (!in.is_open())
   {
      throw std::logic_error("Can't open input file to read values.");
   }

   in.seekg(startPosition);
   values.resize(segment / sizeof(unsigned));
   in.read(reinterpret_cast<char*>(values.data()), sizeof(unsigned) * values.size());
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
   std::vector<unsigned> result(v1.size() + v2.size());
   std::merge(v1.begin(), v1.end(), v2.begin(), v2.end(), result.begin());
   return result;
}

void sortSubsequence(std::vector<unsigned>& result, const std::vector<std::vector<unsigned>>& values, const size_t left, const size_t right)
{
   for (size_t i = left; i < right; ++i)
   {
      result = mergeSortedVectors(result, values[i]);
   }
}

void writeSortedVectorToFile(const std::string& fileName, const std::vector<unsigned>& result)
{
   std::ofstream out(fileName, std::ios::binary);
   if (!out.is_open())
   {
      throw std::ios::failure("Can't open output file to write sorted values.");
   }
   //std::copy(std::begin(result), std::end(result), std::ostream_iterator<int>(out, " "));
   out.write(reinterpret_cast<const char*>(result.data()), sizeof(unsigned) * result.size());
   out.close();
}

void mergeAllSortedVectors(std::vector<unsigned>& result, std::vector<std::vector<unsigned>>& values)
{
   size_t start = 0;
   size_t middle = values.size() / 2;
   size_t end = values.size();

   std::vector<unsigned> left(0);
   std::vector<unsigned> right(0);
   std::unique_ptr<std::thread> leftThread = std::make_unique<std::thread>(&sortSubsequence, std::ref(left), std::cref(values), start, middle);
   std::unique_ptr<std::thread> rightThread = std::make_unique<std::thread>(&sortSubsequence, std::ref(right), std::cref(values), middle, end);

   if (leftThread->joinable())
   {
      leftThread->join();
   }
   if (rightThread->joinable())
   {
      rightThread->join();
   }

   values.clear();

   result = mergeSortedVectors(left, right);
}

void sortVectors(std::vector<std::vector<unsigned>>& values)
{
   int counter = 0;
   while (counter < values.size())
   {
      std::vector<std::unique_ptr<std::thread>> threads;
      for (int i = 0; i < maxThreadsCount; ++i)
      {
         if (counter == values.size())
         {
            break;
         }
         threads.emplace_back(std::make_unique<std::thread>([&vec = values[counter]](){ std::sort(vec.begin(), vec.end()); }));
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
}

void fillVectorsFromFile(const std::string& fileName, std::vector<std::vector<unsigned>>& values, const long long fileLength)
{
   int counter = 0;
   long long startPosition = 0;
   while (startPosition < fileLength)
   {
      std::vector<std::unique_ptr<std::thread>> threads;
      for (int i = 0; i < maxThreadsCount; ++i)
      {
         if (startPosition >= fileLength)
         {
            break;
         }
         const long long segment = fileLength - startPosition >= segmentSize ? segmentSize : fileLength - startPosition;
         threads.emplace_back(std::make_unique<std::thread>(std::bind(&readValuesFromFile, std::cref(fileName), std::ref(values[counter]), startPosition, segment)));
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
}