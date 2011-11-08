package Kafka::MessageSet;

use strict;
use Kafka::Message;
use Carp;

sub new {
    my $class = shift;
    my $stream = shift;

    my $self = {
        valid_byte_count    => 0
    };

    my $data;
    while (<$stream>) {
        $data .= $_;
    }
    my $len = length($data);
    my $ptr = 0;
    while ( $ptr <= ($len-4) ) {
        my $size = unpack('N', substr($data, $ptr, 4));
        $ptr += 4;
        push @{$self->{'messages'}}, Kafka::Message->new(data => substr($data, $ptr, $size));
        $ptr += $size;
        $self->{'valid_byte_count'} += 4 + $size;
    }
}

1;
