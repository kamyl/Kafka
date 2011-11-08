package Kafka::BoundedByteBuffer::Recv;

use strict;
use Carp;
use Fcntl;

sub new {
    my $class = shift;
    my $max_size = shift || 2147483647;
    my $self = {
        max_size    => $max_size
    };
    return bless $self;
}

sub read_request_size {
    my $self = shift;
    my $stream = shift;

    if ( ! $self->{'size_read'} ) {
        read($stream, $self->{'size'}, 4);
        if ( ! $self->{'size'} ) {
            croak "Could not read from stream";
        }
        $self->{'size'} = unpack('N', $self->{'size'});
        if ( $self->{'size'} <= 0 || $self->{'size'} > $self->{'max_size'} ) {
            croak "Message is not a valid size";
        }
        $self->{'remaining_bytes'} = $self->{'size'};
        $self->{'size_read'} = 1;
        return 4;
    }
    return 0;
}

sub read_from {
    my $self = shift;
    my $stream = shift;

    my $read = $self->read_request_size($stream);
    if ( ! $self->{'buffer'} ) {
        open($self->{'buffer'}, ">", undef);
    }
    if ( $self->{'buffer'} && ! $self->{'complete'} ) {
        my $buff_size = min(8192, $self->{'remaining_bytes'});
        if ( $buff_size > 0 ) {
            my $tmp;
            my $actual_read = read($stream, $tmp, $buff_size);
            syswrite($self->{'buffer'}, $tmp);
            $self->{'remaining_bytes'} -= $actual_read;
            $read += $actual_read;
        }

        if ( $self->{'remaining_bytes'} <= 0 ) {
            seek($self->{'buffer'}, 0, 0);
            $self->{'complete'} = 1;
        }
    }
    return $read;
}

sub read_completely {
    my $self = shift;
    my $stream = shift;

    my $read = 0;
    while ( ! $self->{'complete'} ) {
        $read += $self->read_from($stream);
    }
    return $read;
}

sub min {
    return $_[$_[0] > $_[1]];
}

1;
