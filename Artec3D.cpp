#include <iostream>
#include <fstream>
#include <random>
#include <thread>
#include <algorithm>
#include <memory>
#include <functional>
#include <climits>
#include <chrono>
#include <mutex>
#include <cstdio>
#include <deque>
#include <vector>

// input binary file size in bytes 
constexpr int generatedFileSize = 2097153024;

// command line arguments count
constexpr int maxArgumentsCount = 3;

// read segment size in bytes
constexpr int segmentSize = 16 * 1024 * 1024;

// max threads count
constexpr int maxThreadsCount = 8;

// mutex
std::mutex mtx;

// generate input binary file
int generateBinaryFile(const std::string& fileName);

// read unsorted values from input binary file and add them to vector, sort vector and write it to other binary file
void readValuesFromFileSortThemAndWriteToOtherFile(const std::string& fileName, std::deque<std::string>& fileNames,
   const long long startPosition, const long long segment, const int counter);

// get total binary file length
const long long getFileLength(const std::string& fileName);

// merge two binary files with sorted values
void mergeTwoFilesWithSortedValues(std::string resultFileName, std::string fileName1, std::string fileName2);

// create binary files with sorted values from initial binary file
void createFilesWithSortedValuesFromBinaryFile(const std::string& fileName, std::deque<std::string>& fileNames, 
   const long long fileLength);

// merge all files with sorted values to result sorted binary file
void mergeAllFilesWithSortedValues(const std::string& fileName, std::deque<std::string>& fileNames);

int main(int argc, char* argv[])
{
   auto begin = std::chrono::steady_clock::now();

   try
   {
      if (argc != maxArgumentsCount)
      {
         throw std::logic_error("Incorrect arguments count");
      }

      //if (generateBinaryFile(argv[1]))
      //{
      //   throw std::logic_error("Can't open input binary file to write random values.");
      //}

      const long long fileLength = getFileLength(argv[1]);
      if (!fileLength)
      {
         throw std::logic_error("Input file is empty.");
      }
      const long long size = fileLength % segmentSize == 0 ? fileLength / segmentSize : fileLength / segmentSize + 1;
      
      std::deque<std::string> fileNames;
      createFilesWithSortedValuesFromBinaryFile(argv[1], fileNames, fileLength);

      mergeAllFilesWithSortedValues(argv[2], fileNames);
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
   std::cout << "Total time: " << seconds.count() << "sec." << std::endl;

   return EXIT_SUCCESS;
}

int generateBinaryFile(const std::string& fileName)
{
   std::fstream out(fileName, std::ios::binary | std::ios::out);
   if (out.is_open())
   {
      std::random_device rd;
      std::mt19937 mt(rd());
      std::uniform_int_distribution<unsigned> dist(0, UINT_MAX);
      for (auto i = 0; i < generatedFileSize / sizeof(unsigned); ++i)
      {
         unsigned value = dist(mt);
         out.write(reinterpret_cast<char*>(&value), sizeof(unsigned));
      }
      out.close();
      return EXIT_SUCCESS;
   }
   return EXIT_FAILURE;
}

void readValuesFromFileSortThemAndWriteToOtherFile(const std::string& fileName, std::deque<std::string>& fileNames,
   const long long startPosition, const long long segment, const int counter)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (!in.is_open())
   {
      throw std::logic_error("Can't open input file to read values.");
   }

   in.seekg(startPosition);
   std::vector<unsigned> values(segment / sizeof(unsigned));
   in.read(reinterpret_cast<char*>(values.data()), sizeof(unsigned) * values.size());
   in.close();

   std::sort(values.begin(), values.end());
   std::string sorteValuesFileName = "file" + std::to_string(counter);

   std::ofstream out(sorteValuesFileName, std::ios::binary);
   if (!out.is_open())
   {
      throw std::ios::failure("Can't open output file to write sorted values.");
   }
   out.write(reinterpret_cast<const char*>(values.data()), sizeof(unsigned) * values.size());
   out.close();

   std::unique_lock<std::mutex> ul(mtx);
   fileNames.push_back(sorteValuesFileName);
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

void mergeTwoFilesWithSortedValues(std::string resultFileName, std::string fileName1, std::string fileName2)
{
   std::string file1 = fileName1;
   std::string file2 = fileName2;

   std::fstream in1(fileName1, std::ios::binary | std::ios::in);
   std::fstream in2(fileName2, std::ios::binary | std::ios::in);
   std::fstream out(resultFileName, std::ios::binary | std::ios::out);

   if (!in1.is_open())
   {
      throw std::logic_error("Can't open first input file to merge.");
   }

   if (!in2.is_open())
   {
      throw std::logic_error("Can't open second input file to merge.");
   }

   if (!out.is_open())
   {
      throw std::logic_error("Can't open output file to save results.");
   }

   unsigned x, y;
   in1.read(reinterpret_cast<char*>(&x), sizeof(unsigned));
   in2.read(reinterpret_cast<char*>(&y), sizeof(unsigned));
   while (in1 && in2)
   {
      if (x <= y)
      {
         out.write(reinterpret_cast<char*>(&x), sizeof(unsigned));
         in1.read(reinterpret_cast<char*>(&x), sizeof(unsigned));
      }
      else
      {
         out.write(reinterpret_cast<char*>(&y), sizeof(unsigned));
         in2.read(reinterpret_cast<char*>(&y), sizeof(unsigned));
      }
   }

   while (in1)
   {
      in1.read(reinterpret_cast<char*>(&x), sizeof(unsigned));
      out.write(reinterpret_cast<char*>(&x), sizeof(unsigned));
   }

   while (in2)
   {
      in2.read(reinterpret_cast<char*>(&y), sizeof(unsigned));
      out.write(reinterpret_cast<char*>(&y), sizeof(unsigned));
   }

   in1.close();
   in2.close();
   out.close();

   remove(fileName1.c_str());
   remove(fileName2.c_str());
}

void mergeAllFilesWithSortedValues(const std::string& fileName, std::deque<std::string>& fileNames)
{
   std::string mergedFileName;
   auto counter = 0;
   while (fileNames.size() > 1)
   {
      const auto halfSize = static_cast<int>(fileNames.size()) / 2;
      const auto threadsCount =  halfSize > maxThreadsCount ? maxThreadsCount : halfSize;
      std::vector<std::unique_ptr<std::thread>> threads;
      for (auto i = 0; i < threadsCount; ++i)
      {
         mergedFileName = "merged_file_" + std::to_string(counter);
         std::string fileName1 = fileNames.front();
         fileNames.pop_front();
         std::string fileName2 = fileNames.front();
         fileNames.pop_front();
         fileNames.push_back(mergedFileName);
         threads.emplace_back(std::make_unique<std::thread>(&mergeTwoFilesWithSortedValues, mergedFileName, fileName1, fileName2));
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
   
   remove(fileName.c_str());
   auto result = rename(mergedFileName.c_str(), fileName.c_str());
   if (result)
   {
      throw std::logic_error("Can't create binary output file with sorted values.");
   }
}

void createFilesWithSortedValuesFromBinaryFile(const std::string& fileName, std::deque<std::string>& fileNames, const long long fileLength)
{
   auto counter = 0;
   long long startPosition = 0;
   while (startPosition < fileLength)
   {
      std::vector<std::unique_ptr<std::thread>> threads;
      for (auto i = 0; i < maxThreadsCount; ++i)
      {
         if (startPosition >= fileLength)
         {
            break;
         }
         const long long segment = fileLength - startPosition >= segmentSize ? segmentSize : fileLength - startPosition;
         threads.emplace_back(std::make_unique<std::thread>(std::bind(&readValuesFromFileSortThemAndWriteToOtherFile,
            std::cref(fileName), std::ref(fileNames), startPosition, segment, counter)));
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