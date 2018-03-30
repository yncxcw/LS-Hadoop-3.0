#include <random>
#include <iostream>
int main()
{
  /* Initialise. Do this once (not for every
     random number). */
  std::random_device rd;
  std::mt19937_64 gen(rd());

  /* This is where you define the number generator for unsigned long long: */
  std::uniform_int_distribution<unsigned long long> dis;

  /* A few random numbers: */    
  for (int n=0; n<100; ++n)
    std::cout << dis(gen)%100 << ' ';
  std::cout << std::endl;
  return 0;
}
