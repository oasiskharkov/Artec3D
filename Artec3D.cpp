#include <iostream>
#include <fstream>
#include <random>
#include <thread>
#include <algorithm>
#include <mutex>
#include <memory>
#include <functional>

// input file size in bytes 
constexpr int generatedFileSize = 200 * 1024 * 1024; 

// command line arguments count
constexpr int maxArgumentsCount = 3;

// read segment size
constexpr int segmentSize = 8 * 1024 * 1024;

constexpr int maxThreadsCount = 8;

// mutex
std::mutex mt;

// generate input file
int generateBinaryFile(const std::string& fileName);

// read values from input file and add them to vector
void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, int startPosition, bool& endOfFile);

int main(int argc, char* argv[])
{
   //if (argc != maxArgumentsCount)
   //{
   //   std::cerr << "Incorrect arguments count" << std::endl;
   //   return EXIT_FAILURE;
   //}

   //if (generateBinaryFile(argv[1]))
   //if(generateBinaryFile("input"))
   //{
   //   std::cerr << "Can't open input file" << std::endl;
   //   return EXIT_FAILURE;
   //}

   std::vector<unsigned> values;

   bool endOfFile = false;
   int startPosition = 0;
   while (!endOfFile)
   {
      std::vector<std::unique_ptr<std::thread>> threads;
      for (int i = 0; i < 4; ++i)
      {
         std::unique_ptr<std::thread> p = std::make_unique<std::thread>(std::bind(&readValuesFromFile, std::cref(argv[1]), std::ref(values), startPosition, std::ref(endOfFile)));
         threads.push_back(std::move(p));
         startPosition += segmentSize;
      }

      for (auto& vp : threads)
      {
         if (vp->joinable())
         {
            vp->join();
         }    
      }
   }

   std::sort(values.begin(), values.end());

   std::ofstream out(argv[2]);
   if (out.is_open())
   {
      for (const auto& v : values)
      {
         out << v << ' ' << std::endl;
      }
      out.close();
   }
   else
   {
      std::cerr << "Can't open output file" << std::endl;
      return EXIT_FAILURE;
   }

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
         out.write((char*)&value, sizeof(unsigned));
      }
      out.close();
      return 0;
   }
   else
   {
      return 1;
   }
}

void readValuesFromFile(const std::string& fileName, std::vector<unsigned>& values, int startPosition, bool& endOfFile)
{
   std::fstream in(fileName, std::ios::binary | std::ios::in);
   if (in.is_open())
   {
      in.seekg(0, in.end);
      int length = in.tellg();
      in.seekg(0, in.beg);
      if (length >= startPosition)
      {
         mt.lock();
         endOfFile = true;
         mt.unlock();
         return;
      }
      int segment = length - startPosition < segmentSize ? length - startPosition : segmentSize;
      in.seekg(startPosition);
      for (int i = 0; i < segment / sizeof(unsigned); ++i)
      {
         unsigned val;
         in.read((char*)&val, sizeof(unsigned));
         mt.lock();
         values.push_back(val);
         mt.unlock();
      }
      std::cout << std::endl;
      in.close();
   }
}
