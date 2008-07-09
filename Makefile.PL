use strict;
use inc::Module::Install;

name('Queue-Q4M');
all_from('lib/Queue/Q4M.pm');

requires('Moose');
requires('DBI');
requires('Scope::Guard');
requires('SQL::Abstract');

no_index(directory => 'misc');

auto_include;
WriteAll;