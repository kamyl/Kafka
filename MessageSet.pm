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
        my $size = unpack('n*', substr($data, $ptr, 4));
        $ptr += 4;
        if ( $size ) {
            push @{$self->{'messages'}}, Kafka::Message->new(data => substr($data, $ptr, $size));
            $ptr += $size;
        }
        $self->{'valid_byte_count'} += 4 + $size;
    }

    return bless $self;
}

sub total_bytes {
    my $self = shift;
    return $self->{'valid_byte_count'};
}

sub get_messages {
    my $self = shift;
    return $self->{'messages'};
}

1;
