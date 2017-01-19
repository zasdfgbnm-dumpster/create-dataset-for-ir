#include "base64.hpp"
#include "3dstruct.hpp"
#include <iostream>

using namespace base64;
using namespace irms;
using namespace std;

int main() {
	string smiles,b64;
	init_ptridx();
	while (cin >> smiles >> b64) {
		if (smiles.empty()||b64.empty())
			break;
		cout << smiles << "\t" << list_atoms::from_mdl(base64_decode(b64)) << endl;
	}
}
