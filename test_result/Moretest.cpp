#include <iostream>
#include <cstdint>
#include <string>

#include "test.h"

class MoreTest : public Test {
private:

	void put(int begin, int end)//not include end
	{
		for (int i = begin;i < end;++i)
		{
			store.put(i, std::string(i, 's'));
		}
	}

	void del(int begin, int end)//not include end
	{
		for (int i = begin;i < end;++i)
		{
			store.del(i);
		}
	}

	void del_last_level()
	{
		store.reset();
		put(0, 25);
		store.force_dump_test();
		put(25, 51);
		store.force_dump_test();
		del(0, 50);
		store.force_dump_test();
	}

public:
	MoreTest(const std::string& dir) : Test(dir)
	{

	}

	void start_test(void* args = NULL) override
	{
		//put(0, 50);
		del_last_level();
	}

	void check()
	{
		for (int i = 0;i < 51;++i)
		{
			std::cout << store.get(i) << std::endl;
		}
		std::cout << "pass" << std::endl;
	}
};

int main(int argc, char* argv[])
{

	MoreTest test("./data_more_test");

	//test.start_test();
	test.check();

	return 0;
}