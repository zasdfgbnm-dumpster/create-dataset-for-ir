#ifndef __3D_STRUCT_HPP__
#define __3D_STRUCT_HPP__

#include <cstdint>
#include <string>
#include <sstream>
#include <iostream>
#include <iomanip>
#include <vector>
#include <map>

namespace irms {

// http://stackoverflow.com/questions/21764826/skipping-expected-characters-like-scanf-with-cin
class skip {
public:
	const char *text;
	skip(const char *text):text(text){}
};
std::istream & operator >> (std::istream & stream, const skip & x) {
	std::ios_base::fmtflags f = stream.flags();
	stream >> std::noskipws;

	char c;
	const char * text = x.text;
	while (stream && *text++)
		stream >> c;

	stream.flags(f);
	return stream;
}

// periodic table
const std::string pt = "H He Li Be B C N O F Ne Na Mg Al Si P S Cl Ar K Ca Sc Ti V Cr Mn Fe Co Ni Cu Zn Ga Ge As Se Br Kr Rb Sr Y Zr Nb Mo Tc Ru Rh Pd Ag Cd In Sn Sb Te I Xe Cs Ba La Ce Pr Nd Pm Sm Eu Gd Tb Dy Ho Er Tm Yb Lu Hf Ta W Re Os Ir Pt Au Hg Tl Pb Bi Po At Rn Fr Ra Ac Th Pa U Np Pu Am Cm Bk Cf Es Fm Md No Lr Rf Db Sg Bh Hs Mt Ds Rg Cn Nh Fl Mc Lv Ts Og";
std::map<std::string,int> ptridx;
void init_ptridx() {
	std::istringstream is(pt);
	std::string elem;
	int8_t i = 1;
	while(is>>elem)
		ptridx[elem] = i++;
}

struct txyz_t {
	float x;
	float y;
	float z;
	int8_t type;
};
std::istream &operator>>(std::istream &is,txyz_t &txyz) {
	return is >> txyz.type >> skip(",") >> txyz.x >> skip(",") >> txyz.y >> skip(",") >> txyz.z >> skip(";");
}
std::ostream &operator<<(std::ostream &os,const txyz_t &txyz) {
	std::ios::fmtflags f(os.flags());
	os << int(txyz.type) << "," << std::setprecision(5) << txyz.x << "," << txyz.y << "," << txyz.z << ";";
	os.flags(f);
	return os;
}

class list_atoms {
public:
	std::vector<txyz_t> list;
	static list_atoms from_mdl(std::string &&);
	list_atoms() = default;
	list_atoms(list_atoms &&rhs):list(std::move(rhs.list)) {}
};
std::istream &operator>>(std::istream &is,list_atoms &list) {
	int n;
	txyz_t txyz;
	is >> n >> skip(";");
	for(int i=0;i<n;i++) {
		is >> txyz;
		list.list.push_back(txyz);
	}
	return is;
}
std::ostream &operator<<(std::ostream &os,const list_atoms &list) {
	os << list.list.size() << ";";
	for(const txyz_t &txyz : list.list)
		os << txyz;
	return os;
}
list_atoms list_atoms::from_mdl(std::string &&mdl) {
	list_atoms l;
	std::string s;
	int n;
	std::istringstream ss(mdl);
	getline(ss,s);
	getline(ss,s);
	getline(ss,s);
	ss >> n;
	getline(ss,s);
	for(int i=0;i<n;i++) {
		txyz_t txyz;
		ss >> txyz.x >> txyz.y >> txyz.z >> s;
		txyz.type = ptridx[s];
		getline(ss,s);
		l.list.push_back(txyz);
	}
	return l;
}

}

#endif
