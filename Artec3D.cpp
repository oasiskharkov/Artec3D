#include <iostream>
#include <fstream>
#include <random>
#include <thread>
#include <algorithm>
#include <memory>
#include <functional>
#include <climits>
#include <chrono>
#include <unordered_map>
#include <map>
#include <mutex>

// input binary file size in bytes 
constexpr long long generatedFileSize = 2097153024;

// command line arguments count
constexpr int maxArgumentsCount = 3;

// read segment size
constexpr int segmentSize = 16 * 1024 * 1024;

// max threads count
constexpr int maxThreadsCount = 6;

// mutex
std::mutex mtx;

// generate input file
int generateBinaryFile(const std::string& fileName);

// get total file length
const long long getFileLength(const std::string& fileName);

// read values from input file and add them to unordered map
void readValuesFromFile(const std::string& fileName, std::unordered_map<unsigned, int>& values, const long long startPosition, const long long segment);

// fill unordered map with values and their quantity
void fillMapWithUnsortedValues(const std::string& fileName, std::unordered_map<unsigned, int>& values, const long long fileLength);

// write sorted values to binary file
void writeSortedValuesToFile(const std::string& fileName, const std::map<unsigned, int>& result);

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

      std::unordered_map<unsigned, int> unsortedValues;
      fillMapWithUnsortedValues(argv[1], unsortedValues, fileLength);

      std::map<unsigned, int> sortedValues;
      sortedValues.insert(unsortedValues.begin(), unsortedValues.end());
      unsortedValues.clear();

      writeSortedValuesToFile(argv[2], sortedValues);
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
   std::cout << "Total time: " << seconds.count() << "s" << std::endl;
  
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

void readValuesFromFile(const std::string& fileName, std::unordered_map<unsigned, int>& values, const long long startPosition, const long long segment)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (!in.is_open())
   {
      throw std::logic_error("Can't open input file to read values.");
   }

   in.seekg(startPosition);
   std::vector<unsigned> numbers(segment / sizeof(unsigned));
   in.read(reinterpret_cast<char*>(numbers.data()), segment);
   in.close();

   for (const auto& n : numbers)
   {
      std::unique_lock<std::mutex> ulock(mtx);
      auto [iterator, success] = values.try_emplace(n, 1);
      if (!success)
      {
         iterator->second++;
      }
   }
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

void writeSortedValuesToFile(const std::string& fileName, const std::map<unsigned, int>& values)
{
   std::ofstream out(fileName, std::ios::binary);
   if (!out.is_open())
   {
      throw std::ios::failure("Can't open output file to write sorted values.");
   }

   for (const auto& vp : values)
   {
      std::vector<unsigned> result(vp.second, vp.first);
      out.write(reinterpret_cast<const char*>(result.data()), sizeof(unsigned) * result.size());
      //std::copy(result.begin(), result.end(), std::ostream_iterator<unsigned>(out, " "));
   }
   out.close();
}

void fillMapWithUnsortedValues(const std::string& fileName, std::unordered_map<unsigned, int>& values, const long long fileLength)
{
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
         threads.emplace_back(std::make_unique<std::thread>(std::bind(&readValuesFromFile, std::cref(fileName), std::ref(values), startPosition, segment)));
         startPosition += segment;
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