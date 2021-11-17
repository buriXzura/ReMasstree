#include<bits/stdc++.h>

using namespace std;

class A{
public:
    int a,b;
    A(){}
    A(const A* B){
        *this = *B;
    }
};

int main() {
    A a,b;
    a.a=1;
    a.b=2;
    b = A(&a);
    cout<<b.a<<b.b<<"\n";
}