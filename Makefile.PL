use strict;
use inc::Module::Install;

name('Queue-Q4M');
all_from('lib/Queue/Q4M.pm');

requires('Class::Accessor::Fast');
requires('DBI');
requires('SQL::Abstract');
requires('UNIVERSAL::require');

auto_install;
WriteAll;