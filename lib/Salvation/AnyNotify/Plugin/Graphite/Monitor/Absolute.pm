package Salvation::AnyNotify::Plugin::Graphite::Monitor::Absolute;

use strict;
use warnings;
use bignum;

use base 'Salvation::AnyNotify::Plugin::Graphite::Monitor';

use JSON ();
use Salvation::Method::Signatures;

use constant {

    POINT_IDX_TIME => 1,
    POINT_IDX_VALUE => 0,
};

method add(
    Str{1,}|ArrayRef[Str{1,}]{1,} :target!, Num :greater, Num :less,
    Str{1,} :from, Str{1,} :to
) {

    my $core = $self -> core();

    $from //= '-10min';
    $to //= 'now';

    return sub {

        my $now = time();
        my $result = $core -> graphite() -> query(
            target => $target,
            from => $from,
            to => $to,
        );

        if( defined $result ) {

            my @warnings = ();

            foreach my $metric ( $result -> all_metrics() ) {

                my $warn = sub {

                    my ( $type, %data ) = @_;

                    push( @warnings, {
                        time => $now,
                        type => "graphite/absolute/${type}",
                        target => $metric -> target(),
                        %data,
                    } );
                };

                my $max_value = undef;
                my $min_value = undef;

                foreach my $point ( sort( {
                        ( $a -> [ POINT_IDX_TIME ] // 0 )
                        <=> ( $b -> [ POINT_IDX_TIME ] // 0 ) } @{ $metric -> datapoints() } ) ) {

                    my $value = $point -> [ POINT_IDX_VALUE ];

                    next unless defined $value;

                    if( defined $max_value ) {

                        if( $max_value < $value ) {

                            $max_value = $value;
                        }

                    } else {

                        $max_value = $value;
                    }

                    if( defined $min_value ) {

                        if( $min_value > $value ) {

                            $min_value = $value;
                        }

                    } else {

                        $min_value = $value;
                    }
                }

                if( defined $greater ) {

                    if(
                        ( defined $max_value && ( $max_value > $greater ) )
                        || ( defined $min_value && ( $min_value > $greater ) )
                    ) {

                        $warn -> ( greater => (
                            max => ( $max_value // $min_value ),
                            threshold => $greater,
                        ) );
                    }
                }

                if( defined $less ) {

                    if(
                        ( defined $min_value && ( $min_value < $less ) )
                        || ( defined $max_value && ( $max_value < $less ) )
                    ) {

                        $warn -> ( less => (
                            min => ( $min_value // $max_value ),
                            threshold => $less,
                        ) );
                    }
                }
            }

            if( scalar( @warnings ) > 0 ) {

                my $bus = $core -> bus();
                my $json = JSON
                    -> new()
                    -> allow_blessed()
                    -> allow_bignum()
                ;

                foreach my $warning ( @warnings ) {

                    $bus -> notify( $warning -> { 'type' }, $json -> encode( $warning ) );
                }
            }
        }
    };
}

1;

__END__
